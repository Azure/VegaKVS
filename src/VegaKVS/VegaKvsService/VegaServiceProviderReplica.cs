// <copyright file="VegaServiceProviderReplica.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// State provider.
    /// </summary>
    public sealed class VegaServiceProviderReplica : IStateProviderReplica, IStateProvider, IDisposable
    {
        private readonly ReplicatorServiceContext serviceContext;

        private readonly OperationLogger operationLogger;

        private FabricReplicator replicator;

        private Func<CancellationToken, Task<bool>> onDataLossAsync;

        private Uri listenerUri;

        private ReplicaRole currentRole;

        private StatefulServiceInitializationParameters initializationParameters;

        private CancellationTokenSource cancellationTokenSource;

        private TaskCompletionSource<object> pumpTcs;

        /// <summary>
        /// Initializes a new instance of the <see cref="VegaServiceProviderReplica"/> class.
        /// </summary>
        /// <param name="serviceContext">Replicator service context.</param>
        public VegaServiceProviderReplica(ReplicatorServiceContext serviceContext)
        {
            this.serviceContext = serviceContext;

            // TODO: read directory name from setting
            this.operationLogger = new OperationLogger(this.serviceContext.ServiceContext.CodePackageActivationContext.LogDirectory);
        }

        /// <inheritdoc />
        public Func<CancellationToken, Task<bool>> OnDataLossAsync
        {
            set
            {
                this.onDataLossAsync = value;
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (this.cancellationTokenSource != null)
            {
                this.cancellationTokenSource.Dispose();
                this.cancellationTokenSource = null;
            }
        }

        /// <inheritdoc />
        public void Abort()
        {
            this.StopProcessing();
            this.Log("Abort");
        }

        /// <inheritdoc />
        public Task BackupAsync(Func<BackupInfo, CancellationToken, Task<bool>> backupCallback)
        {
            // TODO
            return Task.FromResult(0);
        }

        /// <inheritdoc />
        public Task BackupAsync(BackupOption option, TimeSpan timeout, CancellationToken cancellationToken, Func<BackupInfo, CancellationToken, Task<bool>> backupCallback)
        {
            // TODO
            return Task.FromResult(0);
        }

        /// <inheritdoc />
        public Task ChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            this.Log("New role: {0}", newRole);
            switch (newRole)
            {
                case ReplicaRole.Unknown:
                    throw new InvalidOperationException("Unexpected ChangeRole");

                case ReplicaRole.Primary:
                    this.cancellationTokenSource = new CancellationTokenSource();
                    break;

                case ReplicaRole.None:
                    this.StopProcessing();
                    break;

                case ReplicaRole.IdleSecondary:
                    this.StartCopyAndReplicationPump();
                    this.StopProcessing();
                    break;

                case ReplicaRole.ActiveSecondary:
                    this.StartReplicationPump();
                    this.StopProcessing();
                    break;
            }

            this.currentRole = newRole;
            return Task.FromResult(this.listenerUri.ToString());
        }

        /// <inheritdoc />
        public Task CloseAsync(CancellationToken cancellationToken)
        {
            this.StopProcessing();
            this.Log("Close");

            return Task.FromResult(0);
        }

        /// <inheritdoc />
        public void Initialize(StatefulServiceInitializationParameters initializationParameters)
        {
            this.initializationParameters = initializationParameters;
            this.currentRole = ReplicaRole.Unknown;
        }

        /// <inheritdoc />
        public Task<IReplicator> OpenAsync(ReplicaOpenMode openMode, IStatefulServicePartition partition, CancellationToken cancellationToken)
        {
            // TODO: open a service host
            this.listenerUri = new Uri("tcp://127.0.0.1:1234");

            var replicatorSettings = this.GetReplicatorSettings();

            ServiceEventSource.Current.ServiceMessage(this.serviceContext.ServiceContext, "ReplicatorSettings: {0}", replicatorSettings.ToString());

            this.replicator = partition.CreateReplicator(this, replicatorSettings);

            this.serviceContext.Replicator = this.replicator;

            return Task.FromResult<IReplicator>(this.replicator);
        }

        /// <inheritdoc />
        public Task RestoreAsync(string backupFolderPath)
        {
            // TODO
            return Task.FromResult(0);
        }

        /// <inheritdoc />
        public Task RestoreAsync(string backupFolderPath, RestorePolicy restorePolicy, CancellationToken cancellationToken)
        {
            // TODO
            return Task.FromResult(0);
        }

        /// <inheritdoc />
        IOperationDataStream IStateProvider.GetCopyContext()
        {
            return new VegaOperationDataStream(0L);
        }

        /// <inheritdoc />
        IOperationDataStream IStateProvider.GetCopyState(long upToSequenceNumber, IOperationDataStream copyContext)
        {
            // TODO: send the data from primary to secondary
            return new VegaOperationDataStream(upToSequenceNumber);
        }

        /// <inheritdoc />
        long IStateProvider.GetLastCommittedSequenceNumber()
        {
            return 0L;
        }

        /// <inheritdoc />
        Task<bool> IStateProvider.OnDataLossAsync(CancellationToken cancellationToken)
        {
            // TODO
            return Task.FromResult(false);
        }

        /// <inheritdoc />
        Task IStateProvider.UpdateEpochAsync(Epoch epoch, long previousEpochLastSequenceNumber, CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        private void StopProcessing()
        {
            if (this.cancellationTokenSource != null)
            {
                this.cancellationTokenSource.Cancel();
                this.cancellationTokenSource.Dispose();
                this.cancellationTokenSource = null;
            }
        }

        private ReplicatorSettings GetReplicatorSettings()
        {
            var replicatorSettings = ReplicatorSettings.LoadFrom(
                (CodePackageActivationContext)this.serviceContext.ServiceContext.CodePackageActivationContext,
                "Config",
                "ReplicatorConfig");

            ////replicatorSettings.ReplicatorAddress
            replicatorSettings.RequireServiceAck = true;
            return replicatorSettings;
        }

        private void Log(string format, params object[] args)
        {
            ServiceEventSource.Current.ServiceMessage(this.serviceContext.ServiceContext, format, args);
        }

        private void StartCopyAndReplicationPump()
        {
            if (this.pumpTcs == null)
            {
                this.pumpTcs = new TaskCompletionSource<object>();
                var copyPump = Task.Run(() => this.DrainCopyAndReplicationStream());
            }
        }

        private void StartReplicationPump()
        {
            if (this.pumpTcs == null)
            {
                this.pumpTcs = new TaskCompletionSource<object>();
                var copyPump = Task.Run(() => this.DrainReplicationStream());
            }
        }

        private async Task DrainCopyAndReplicationStream()
        {
            try
            {
                var stream = this.replicator.StateReplicator.GetCopyStream();

                do
                {
                    var op = await stream.GetOperationAsync(CancellationToken.None).ConfigureAwait(false);
                    if (op == null)
                    {
                        break;
                    }

                    await this.operationLogger.WriteOperations(op);

                    await this.serviceContext.ProcessReplicatedOperation.Invoke(op).ConfigureAwait(false);
                    Task ackTask = Task.Run(() => op.Acknowledge());
                }
                while (true);

                stream = this.replicator.StateReplicator.GetReplicationStream();

                do
                {
                    var op = await stream.GetOperationAsync(CancellationToken.None).ConfigureAwait(false);
                    if (op == null)
                    {
                        break;
                    }

                    await this.operationLogger.WriteOperations(op);

                    // consume data on Secondary, op.Data, op.SequenceNumber
                    await this.serviceContext.ProcessReplicatedOperation.Invoke(op).ConfigureAwait(false);
                    Task ackTask = Task.Run(() => op.Acknowledge());
                }
                while (true);
            }
            catch (Exception e)
            {
                this.Log("DrainCopyAndReplicationStream: exception while draining copy and repl stream: {0]", e);
            }
            finally
            {
                this.pumpTcs.SetResult(null);
                this.pumpTcs = null;
            }
        }

        private async Task DrainReplicationStream()
        {
            try
            {
                var stream = this.replicator.StateReplicator.GetReplicationStream();
                do
                {
                    var op = await stream.GetOperationAsync(CancellationToken.None).ConfigureAwait(false);
                    if (op == null)
                    {
                        break;
                    }

                    await this.operationLogger.WriteOperations(op);

                    // consume data on Secondary, op.Data, op.SequenceNumber
                    var unused = this.serviceContext.ProcessReplicatedOperation.Invoke(op)
                        .ContinueWith(t => op.Acknowledge());
                }
                while (true);
            }
            catch (Exception e)
            {
                this.Log("Exception {0} while draining copy and repl stream", e.Message, e.StackTrace, e.HResult);
            }
            finally
            {
                this.pumpTcs.SetResult(null);
                this.pumpTcs = null;
            }
        }
    }
}
