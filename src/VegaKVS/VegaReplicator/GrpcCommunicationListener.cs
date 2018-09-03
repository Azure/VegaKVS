// <copyright file="GrpcCommunicationListener.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Grpc.Core;
    using Microsoft.ServiceFabric.Services.Communication.Runtime;

    /// <summary>
    /// Communicator listener using gRPC.
    /// </summary>
    public sealed class GrpcCommunicationListener : ICommunicationListener
    {
        private readonly Func<ServerServiceDefinition> createService;
        private readonly string hostIpAddress;
        private Grpc.Core.Server server;

        /// <summary>
        /// Initializes a new instance of the <see cref="GrpcCommunicationListener"/> class.
        /// </summary>
        /// <param name="createService">Function to create grpc service.</param>
        /// <param name="ipAddressOrFqdn">IP address of FQDN of the node.</param>
        public GrpcCommunicationListener(Func<ServerServiceDefinition> createService, string ipAddressOrFqdn)
        {
            this.createService = createService;
            this.hostIpAddress = ipAddressOrFqdn;
        }

        /// <inheritdoc />
        public void Abort() => this.server.KillAsync().GetAwaiter().GetResult();

        /// <inheritdoc />
        public Task CloseAsync(CancellationToken cancellationToken) => this.server.ShutdownAsync();

        /// <inheritdoc />
        public Task<string> OpenAsync(CancellationToken cancellationToken)
        {
            var serviceDefinition = this.createService();
            this.server = new Grpc.Core.Server
            {
                Services =
                {
                    serviceDefinition,
                },
                Ports =
                {
                    new Grpc.Core.ServerPort("0.0.0.0", Grpc.Core.ServerPort.PickUnused, Grpc.Core.ServerCredentials.Insecure),
                },
            };

            this.server.Start();

            int workerCount, completionPortCount;
            ThreadPool.GetMinThreads(out workerCount, out completionPortCount);
            ThreadPool.SetMinThreads(64, completionPortCount);

            return Task.FromResult($"grpc://{this.hostIpAddress}:{this.server.Ports.First().BoundPort}");
        }
    }
}
