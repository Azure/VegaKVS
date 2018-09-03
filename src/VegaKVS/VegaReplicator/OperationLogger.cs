// <copyright file="OperationLogger.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.IO;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Writes operation data to log files.
    /// </summary>
    public sealed class OperationLogger : IOperationLogger
    {
        /// <summary>
        /// OS default write buffer size is 4 KB. This buffer should be sufficient so the write operation happens only
        /// when calling flush.
        /// </summary>
        private readonly int logFileBufferSize = 1024 * 1024;

        private readonly int logFileSize = 1024 * 1024 * 20;

        private readonly string logDirectory;

        private FileStream currentLogFile;

        /// <summary>
        /// Initializes a new instance of the <see cref="OperationLogger"/> class.
        /// </summary>
        /// <param name="logDirectory">The directory at where log files and checkpoint files are located.</param>
        public OperationLogger(string logDirectory)
        {
            this.logDirectory = logDirectory;
        }

        /// <inheritdoc />
        public void Close()
        {
            this.currentLogFile.Close();
        }

        /// <inheritdoc />
        public Task ReadOperations(long sinceSequenceNumber, Action<long, OperationData> processOperationData, CancellationToken cancellation) => throw new NotImplementedException();

        /// <inheritdoc />
        public Task WriteOperations(IOperation op)
        {
            return this.WriteOperations(op.SequenceNumber, op.Data);
        }

        /// <inheritdoc />
        public Task WriteOperations(long sequenceNumber, OperationData opData)
        {
            this.OpenLogFileIfNeeded(sequenceNumber);

            this.currentLogFile.Write(BitConverter.GetBytes(sequenceNumber));
            this.currentLogFile.Write(BitConverter.GetBytes(opData.Count));

            foreach (var data in opData)
            {
                this.currentLogFile.Write(BitConverter.GetBytes(data.Count));

                if (data.Count > 0)
                {
                    this.currentLogFile.Write(data.Array, data.Offset, data.Count);
                }
            }

            //// ServiceEventSource.Current.Debug($"WriteOp in {this.logDirectory} seq {sequenceNumber}");

            return Task.Run(() => this.currentLogFile.Flush(true));
        }

        private void OpenLogFileIfNeeded(long sequenceNumber)
        {
            if (this.currentLogFile == null)
            {
                var filename = Path.Combine(this.logDirectory, $"{sequenceNumber}{DateTime.UtcNow.ToString("--yyMMdd-HHmmss.fff")}.log");
                this.currentLogFile = new FileStream(
                    filename,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.Read,
                    this.logFileBufferSize);

                ServiceEventSource.Current.Message("Open log file: {0}", filename);
            }
            else if (this.currentLogFile.Length >= this.logFileSize)
            {
                this.currentLogFile.Close();

                var filename = Path.Combine(this.logDirectory, $"{sequenceNumber}{DateTime.UtcNow.ToString("--yyMMdd-HHmmss.fff")}.log");
                this.currentLogFile = new FileStream(
                    filename,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.Read,
                    this.logFileBufferSize);

                ServiceEventSource.Current.Message("Open log file: {0}", filename);
            }
        }
    }
}
