// <copyright file="VegaOperationDataStream.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Data stream for exchanging data between primary and secondaries.
    /// </summary>
    public class VegaOperationDataStream : IOperationDataStream
    {
        private readonly long lsn;
        private long sequence;

        /// <summary>
        /// Initializes a new instance of the <see cref="VegaOperationDataStream"/> class.
        /// </summary>
        /// <param name="lsn">Logical sequence number.</param>
        public VegaOperationDataStream(long lsn)
        {
            this.lsn = lsn;
            this.sequence = 1L; // starting from 1, not 0.
        }

        /// <inheritdoc />
        public Task<OperationData> GetNextAsync(CancellationToken cancellationToken)
        {
            return Task.Run<OperationData>(
                async () =>
                {
                    if (this.sequence > this.lsn)
                    {
                        return null;
                    }

                    await Task.Yield();
                    var opData = new OperationData(BitConverter.GetBytes(this.sequence++));
                    return opData;
                });
        }
    }
}