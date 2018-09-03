// <copyright file="VegaKvsService.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data.Collections;
    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using Microsoft.ServiceFabric.Services.Runtime;

    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class VegaKvsService : StatefulServiceBase, IDisposable
    {
        private readonly MultiVersionDictionary<byte[]> kvsData;
        private readonly ReplicatorServiceContext serviceContext;
        private readonly BlockingCollection<PendingOperation> pendingOperations;

        private CancellationTokenSource cancellationTokenSource;
        private IStateReplicator stateReplicator;
        private OperationLogger operationLoggerPrimary;

        /// <summary>
        /// Most recent sequence number that denotes the committed operations on primary. On secondaries this refers
        /// to the last known primary sequence number, which is one less than the sequence number from replicated
        /// operations.
        /// </summary>
        private long lastSequenceNumber;

        /// <summary>
        /// Initializes a new instance of the <see cref="VegaKvsService"/> class.
        /// </summary>
        /// <param name="context">Replicator service context.</param>
        public VegaKvsService(ReplicatorServiceContext context)
            : base(context.ServiceContext, new VegaServiceProviderReplica(context))
        {
            this.kvsData = new MultiVersionDictionary<byte[]>();
            this.lastSequenceNumber = 0;

            // TODO: set a upper-limit
            this.pendingOperations = new BlockingCollection<PendingOperation>();

            this.serviceContext = context;
            this.serviceContext.ProcessReplicatedOperation = this.ProcessReplicatedOperation;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.cancellationTokenSource?.Dispose();
            this.pendingOperations.Dispose();
        }

        /// <summary>
        /// Reads the value of a node for the given key.
        /// </summary>
        /// <param name="key">Key of the node in string.</param>
        /// <returns>Value of the node.</returns>
        internal Tuple<bool, byte[]> Read(string key)
        {
            byte[] value;
            var succeeded = this.kvsData.TryGetValue(key, this.lastSequenceNumber, out value);
            return Tuple.Create(succeeded, value);
        }

        /// <summary>
        /// Creates a new node or updates an existing or delete a node.
        /// </summary>
        /// <param name="kind">Kind of operation.</param>
        /// <param name="key">Key of the node.</param>
        /// <param name="value">Value of the node.</param>
        /// <param name="onCommit">Action performed when the replication is completed.</param>
        /// <param name="onFailure">Action performed when the replication is failed.</param>
        internal void Write(NodeOperation.OperationKind kind, string key, ArraySegment<byte> value, Action onCommit, Action onFailure)
        {
            var nodeOp = new NodeOperation(kind, this.lastSequenceNumber, key, value);
            var pendingOp = new PendingOperation { Operation = nodeOp, OnCommit = onCommit, OnFailure = onFailure, };
            this.pendingOperations.Add(pendingOp);
        }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication.
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[]
            {
                new ServiceReplicaListener(
                    context => new GrpcCommunicationListener(
                        () => VegaKvs.Proto.KeyValueStore.BindService(new KeyValueStoreImpl(this)),
                        this.Context.NodeContext.IPAddressOrFQDN),
                    name: "VegaKvsGrpc",
                    listenOnSecondary: true),
            };
        }

        /// <inheritdoc/>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // Find a better log directory
            this.operationLoggerPrimary = new OperationLogger(this.Context.CodePackageActivationContext.LogDirectory);

            FabricReplicator replicator = null;

            while ((replicator = this.serviceContext.Replicator) == null)
            {
                await Task.Delay(125).ConfigureAwait(false);
            }

            this.stateReplicator = replicator.StateReplicator;
            this.cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            var unusedTask = Task.Run(this.ProcessIncomingRequests);

            ServiceEventSource.Current.ServiceMessage(this.Context, "Replicator is ready");

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    ServiceEventSource.Current.ServiceMessage(
                        this.Context,
                        "RunAsync alive: {0} - pending operations {1} LSN {2}",
                        DateTime.UtcNow,
                        this.pendingOperations.Count,
                        this.lastSequenceNumber);
                    await Task.Delay(1000 * 30, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (TaskCanceledException)
            {
            }
        }

        private Task<bool> ProcessReplicatedOperation(IOperation op)
        {
            var succeeded = true;
            foreach (var data in op.Data)
            {
                foreach (var nodeOp in NodeOperation.GetNodeOperationFromBytes(data))
                {
                    ////ServiceEventSource.Current.ServiceMessage(this.Context, "Received #{0} - {1}", sequenceNumber, nodeOp);

                    switch (nodeOp.Operation)
                    {
                        case NodeOperation.OperationKind.Nop:
                            // No operation, primary is telling its sequence number to secondaries.
                            break;

                        case NodeOperation.OperationKind.Create:
                        case NodeOperation.OperationKind.Update:
                            var valueArray = new byte[nodeOp.Value.Count];
                            Array.Copy(nodeOp.Value.Array, nodeOp.Value.Offset, valueArray, 0, nodeOp.Value.Count);

                            succeeded &= this.kvsData.AddOrUpdate(nodeOp.Key, nodeOp.SequenceNumber, valueArray);
                            break;

                        case NodeOperation.OperationKind.Delete:
                            succeeded &= this.kvsData.TryRemove(nodeOp.Key, nodeOp.SequenceNumber);
                            break;

                        default:
                            ServiceEventSource.Current.ServiceMessage(this.Context, $"Unknown node operation: {nodeOp}");
                            break;
                    }
                }
            }

            ServiceEventSource.Current.ServiceMessage(this.Context, $"Process replicated op, SN={op.SequenceNumber} succeeded={succeeded}");
            Interlocked.Exchange(ref this.lastSequenceNumber, op.SequenceNumber - 1);

            // The caller is supposed to ack the operation
            return Task.FromResult(succeeded);
        }

        private async Task ProcessIncomingRequests()
        {
            CancellationToken cancellation = this.cancellationTokenSource.Token;
            var replicationTaskSem = new SemaphoreSlim(128, 128);

            try
            {
                while (!cancellation.IsCancellationRequested)
                {
                    var ops = new List<PendingOperation>();
                    var op = this.pendingOperations.Take(cancellation);

                    do
                    {
                        ops.Add(op);
                    }
                    while (this.pendingOperations.TryTake(out op));

                    var opData = NodeOperation.GetOperationData(ops.Select(o => o.Operation));

                    try
                    {
                        await replicationTaskSem.WaitAsync(cancellation).ConfigureAwait(false);

                        var replicateTask = this.stateReplicator.ReplicateAsync(opData, cancellation, out long lsn);
                        var unsed = replicateTask.ContinueWith(
                            t =>
                            {
                                replicationTaskSem.Release();

                                foreach (var operation in ops)
                                {
                                    switch (operation.Operation.Operation)
                                    {
                                        case NodeOperation.OperationKind.Nop:
                                            // No operation, primary is telling its sequence number to secondaries.
                                            break;

                                        case NodeOperation.OperationKind.Create:
                                        case NodeOperation.OperationKind.Update:
                                            var data = new byte[operation.Operation.Value.Count];
                                            Array.Copy(operation.Operation.Value.Array, operation.Operation.Value.Offset, data, 0, operation.Operation.Value.Count);
                                            this.kvsData.AddOrUpdate(operation.Operation.Key, operation.Operation.SequenceNumber, data);
                                            break;

                                        case NodeOperation.OperationKind.Delete:
                                            this.kvsData.TryRemove(operation.Operation.Key, operation.Operation.SequenceNumber);
                                            break;

                                        default:
                                            throw new InvalidOperationException();
                                    }

                                    operation.OnCommit();
                                }
                            });

                        // Writes to disk while the replication to secondaries is started.
                        await this.operationLoggerPrimary.WriteOperations(lsn, opData);

                        ServiceEventSource.Current.ServiceMessage(this.Context, $"Replicated and persisted on primary, SN={lsn}");
                        Interlocked.Exchange(ref this.lastSequenceNumber, lsn);
                    }
                    catch (Exception ex) when (!(ex is OperationCanceledException))
                    {
                        foreach (var operation in ops)
                        {
                            operation.OnFailure();
                        }
                    }
                }

                SpinWait.SpinUntil(() => replicationTaskSem.CurrentCount == 128);
            }
            catch (OperationCanceledException)
            {
                ServiceEventSource.Current.ServiceMessage(this.Context, "ProcessIncomingRequests operation cancelled.");
            }
            catch (Exception ex)
            {
                ServiceEventSource.Current.ServiceMessage(this.Context, "ProcessIncomingRequests unhandled exception: {0}", ex);
            }
            finally
            {
                ServiceEventSource.Current.ServiceMessage(this.Context, "ProcessIncomingRequests terminated.");
            }
        }

        private struct PendingOperation
        {
            public NodeOperation Operation;
            public Action OnCommit;
            public Action OnFailure;
        }
    }
}
