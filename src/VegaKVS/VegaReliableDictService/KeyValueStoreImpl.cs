// <copyright file="KeyValueStoreImpl.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Google.Protobuf;
    using Grpc.Core;
    using Microsoft.VegaKvs.Proto;

    /// <summary>
    /// Implementation of VegaKvs gRPC service.
    /// </summary>
    internal sealed class KeyValueStoreImpl : KeyValueStore.KeyValueStoreBase
    {
        private readonly VegaReliableDictService service;

        /// <summary>
        /// Initializes a new instance of the <see cref="KeyValueStoreImpl"/> class.
        /// </summary>
        /// <param name="service">Stateful service.</param>
        public KeyValueStoreImpl(VegaReliableDictService service)
        {
            this.service = service;
        }

        /// <inheritdoc />
        public override Task<BoolResponse> Echo(KeyRequest request, ServerCallContext context)
        {
            var result = new BoolResponse
            {
                Succeeded = !string.IsNullOrEmpty(request.Key),
            };

            return Task.FromResult(result);
        }

        /// <inheritdoc/>
        public override async Task<NodeResponse> Read(KeyRequest request, ServerCallContext context)
        {
            var getResult = await this.service.Read(request.Key);
            var value = getResult.Item2 == null
                ? ByteString.Empty
                : ByteString.CopyFrom(getResult.Item2);
            var response = new NodeResponse
            {
                Node = new Node
                {
                    Key = request.Key,
                    Value = value,
                },
                Succeeded = getResult.Item1,
            };

            return response;
        }

        /// <inheritdoc />
        public override async Task ReadStream(IAsyncStreamReader<KeyRequest> requestStream, IServerStreamWriter<NodeResponse> responseStream, ServerCallContext context)
        {
            while (await requestStream.MoveNext(CancellationToken.None))
            {
                var key = requestStream.Current.Key;
                var getResult = await this.service.Read(key);
                var value = getResult.Item2 == null
                    ? ByteString.Empty
                    : ByteString.CopyFrom(getResult.Item2);
                var response = new NodeResponse
                {
                    Node = new Node
                    {
                        Key = key,
                        Value = value,
                    },
                    Succeeded = getResult.Item1,
                };

                await responseStream.WriteAsync(response);
            }
        }

        /// <inheritdoc/>
        public override async Task<BoolResponse> Create(Node request, ServerCallContext context)
        {
            var succeeded = await this.service.Write(
                NodeOperation.OperationKind.Create,
                request.Key,
                request.Value.ToByteArray());
            return new BoolResponse { Succeeded = succeeded, };
        }

        /// <inheritdoc/>
        public override async Task<BoolResponse> Update(Node request, ServerCallContext context)
        {
            var succeeded = await this.service.Write(
                NodeOperation.OperationKind.Update,
                request.Key,
                request.Value.ToByteArray());
            return new BoolResponse { Succeeded = succeeded, };
        }

        /// <inheritdoc/>
        public override async Task<BoolResponse> Delete(KeyRequest request, ServerCallContext context)
        {
            var succeeded = await this.service.Write(
                NodeOperation.OperationKind.Delete,
                request.Key,
                default(ArraySegment<byte>));
            return new BoolResponse { Succeeded = succeeded, };
        }
    }
}