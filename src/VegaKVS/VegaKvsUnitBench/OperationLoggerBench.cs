// <copyright file="OperationLoggerBench.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Diagnostics;
    using System.Fabric;
    using System.IO;
    using System.Linq;

    /// <summary>
    /// Measures the performance of <see cref="OperationLogger"/> write.
    /// </summary>
    internal sealed class OperationLoggerBench : IBench
    {
        private const int OperationCount = 10_000;
        private const int PayloadSize = 1 * 1024;

        /// <inheritdoc />
        public void Run(Action<string> log)
        {
            var dir = Path.GetTempPath();
            try
            {
                var logger = new OperationLogger(dir);

                var payload = Enumerable.Range(0, PayloadSize).Select(i => (byte)i).ToArray();
                var sw = new Stopwatch();
                int gen0 = GC.CollectionCount(0), gen1 = GC.CollectionCount(1), gen2 = GC.CollectionCount(2);
                sw.Start();

                for (int i = 0; i < OperationCount; i++)
                {
                    var op = new OperationData(payload);
                    logger.WriteOperations(i, op).GetAwaiter().GetResult();
                }

                logger.Close();
                sw.Stop();

                var rate = OperationCount / sw.Elapsed.TotalSeconds;
                log($"OPerationLogger: {sw.Elapsed} QPS={rate:G3}");
                log($"  Gen0={GC.CollectionCount(0) - gen0} Gen1={GC.CollectionCount(1) - gen1} Gen2={GC.CollectionCount(2) - gen2}\n");
            }
            finally
            {
                File.Delete(Path.Combine(dir, "0.log"));
            }
        }
    }
}
