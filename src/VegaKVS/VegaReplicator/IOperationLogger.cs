// <copyright file="IOperationLogger.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Writes operation data to log files during normal operation and reads operation data for rebuilding secondaries.
    /// </summary>
    public interface IOperationLogger
    {
        /// <summary>
        /// Closes the current log file and stops accepting further writes.
        /// </summary>
        void Close();

        /// <summary>
        /// Writes an operation received from primary to disk. This is called by secondary replicas.
        /// </summary>
        /// <param name="op">Operation to write.</param>
        /// <returns>Async task to indicate the completion of writing to disk.</returns>
        Task WriteOperations(IOperation op);

        /// <summary>
        /// Writes an operation received from primary to disk. This is called by primary replica.
        /// </summary>
        /// <param name="sequenceNumber">Last sequence number on primary.</param>
        /// <param name="opData">OperationData to write.</param>
        /// <returns>Async task to indicate the completion of writing to disk.</returns>
        Task WriteOperations(long sequenceNumber, OperationData opData);

        /// <summary>
        /// Reads the log files and calls client provided operation data processing action.
        /// </summary>
        /// <param name="sinceSequenceNumber">Ignore all operation data with sequence number equal or less than this.</param>
        /// <param name="processOperationData">Client provided processing action.</param>
        /// <param name="cancellation">Cancellation token to cancel the reading.</param>
        /// <returns>Async task to indicate the completion of reading.</returns>
        Task ReadOperations(long sinceSequenceNumber, Action<long, OperationData> processOperationData, CancellationToken cancellation);
    }
}
