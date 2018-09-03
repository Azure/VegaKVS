// <copyright file="VegaReliableDictService.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
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
    internal sealed class VegaReliableDictService : StatefulService
    {
        private IReliableDictionary2<string, byte[]> fabricDataStore;

        /// <summary>
        /// Initializes a new instance of the <see cref="VegaReliableDictService"/> class.
        /// </summary>
        /// <param name="context">Service context.</param>
        public VegaReliableDictService(StatefulServiceContext context)
            : base(context)
        {
        }

        /// <summary>
        /// Reads the value of a node for the given key.
        /// </summary>
        /// <param name="key">Key of the node in string.</param>
        /// <returns>Value of the node.</returns>
        internal async Task<Tuple<bool, byte[]>> Read(string key)
        {
            using (var tx = this.StateManager.CreateTransaction())
            {
                var data = await this.fabricDataStore.TryGetValueAsync(tx, key);

                var result = Tuple.Create(data.HasValue, data.Value);
                await tx.CommitAsync().ConfigureAwait(false);

                return result;
            }
        }

        /// <summary>
        /// Creates a new node or updates an existing or delete a node.
        /// </summary>
        /// <param name="kind">Kind of operation.</param>
        /// <param name="key">Key of the node.</param>
        /// <param name="value">Value of the node.</param>
        /// <returns>Async task to indicate the completion of write.</returns>
        internal async Task<bool> Write(NodeOperation.OperationKind kind, string key, ArraySegment<byte> value)
        {
            bool succeeded = false;

            using (var tx = this.StateManager.CreateTransaction())
            {
                var data = await this.fabricDataStore.TryGetValueAsync(tx, key);
                switch (kind)
                {
                    case NodeOperation.OperationKind.Create:
                        succeeded = await this.fabricDataStore.TryAddAsync(tx, key, value.ToArray());
                        break;

                    case NodeOperation.OperationKind.Delete:
                        succeeded = (await this.fabricDataStore.TryRemoveAsync(tx, key)).HasValue;
                        break;

                    case NodeOperation.OperationKind.Update:
                        succeeded = await this.fabricDataStore.TryUpdateAsync(tx, key, value.ToArray(), data.Value);
                        break;
                }

                await tx.CommitAsync().ConfigureAwait(false);
            }

            return succeeded;
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

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        /// <returns>Async task to indicate the completion of this method.</returns>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            this.fabricDataStore = await this.StateManager.GetOrAddAsync<IReliableDictionary2<string, byte[]>>("FabricDataStore").ConfigureAwait(false);
        }
    }
}
