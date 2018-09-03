// <copyright file="VegaKvsClient.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Fabric;
    using System.Linq;
    using System.Net;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Google.Protobuf;
    using Grpc.Core;
    using static Microsoft.VegaKvs.Proto.KeyValueStore;

    /// <summary>
    /// Mini client to test Vega KVS.
    /// </summary>
    internal sealed class VegaKvsClient
    {
        private const string VegaKvsAppTypeName = "VegaKvsAppType";
        private const string VegaKvsServiceTypeName = "VegaKvsServiceType";
        private const int AsyncTaskCount = 64;

        private static readonly Action<string> Log = (s) => Console.WriteLine(s);

        /// <summary>
        /// Total amount of data being processed.
        /// </summary>
        private static long totalDataSize = 0;

        /// <summary>
        /// Total number of data items being processed.
        /// </summary>
        private static long totalDataCount = 0;

        /// <summary>
        /// Total number of failures in each test.
        /// </summary>
        private static long totalFailures = 0;

        private static async Task Main(string[] args)
        {
            if (args.Length < 2)
            {
                Log("USAGE: VegaKvsClient [endpoint] [scenario]\n" +
                    "Scenario: PingPong, Create, Read, ReadStream, Mixed-High, Mixed-Low, Delete, ReadAllReplica, ReadAllReplicaStream\n" +
                    "Endpoint: auto for auto-discovery, or [IPAddress:Port]\n");
                return;
            }

            var endpoint = await ParseOrFindEndpoint(args[0] == "auto" ? string.Empty : args[0]);

            // actual test starts here
            var testCase = args[1];

            if (testCase == "PingPong")
            {
                PingPongTest(64, endpoint);
            }
            else if (testCase == "Create")
            {
                CreateTest(64, endpoint);
            }
            else if (testCase == "Read")
            {
                ReadTest(64, endpoint);
            }
            else if (testCase == "ReadStream")
            {
                ReadStreamTest(64, endpoint);
            }
            else if (testCase == "Mixed-High")
            {
                MixedRWTest(64, endpoint, 0.1);
            }
            else if (testCase == "Mixed-Low")
            {
                MixedRWTest(64, endpoint, 0.01);
            }
            else if (testCase == "Delete")
            {
                DeleteTest(64, endpoint);
            }
            else if (testCase == "ReadAllReplica")
            {
                var endpoints = await FindAllEndpoints();
                await ReadAllReplica(64, endpoints);
            }
            else if (testCase == "ReadAllReplicaStream")
            {
                var endpoints = await FindAllEndpoints();
                await ReadAllReplicaStream(64, endpoints);
            }
        }

        private static async Task<Tuple<string, int>> ParseOrFindEndpoint(string s)
        {
            if (!string.IsNullOrWhiteSpace(s))
            {
                var elements = s.Split(':');
                if (elements.Length != 2)
                {
                    throw new ArgumentException("Endpoint cannot parsed", nameof(s));
                }

                return Tuple.Create(elements[0], int.Parse(elements[1]));
            }
            else
            {
                using (var fabricClient = new FabricClient())
                {
                    var qm = fabricClient.QueryManager;
                    var app = (await qm.GetApplicationListAsync())
                        .First(a => a.ApplicationTypeName == VegaKvsAppTypeName);
                    var service = (await qm.GetServiceListAsync(app.ApplicationName))
                        .First(svc => svc.ServiceTypeName == VegaKvsServiceTypeName);
                    var endpoint = (await fabricClient.ServiceManager.ResolveServicePartitionAsync(service.ServiceName))
                        .Endpoints
                        .First(ep => ep.Role == ServiceEndpointRole.StatefulPrimary);

                    // endpoint address looks like: {"Endpoints":{"VegaKvsGrpc":"grpc:\/\/zy.redmond.corp.microsoft.com:64186"}}
                    var match = Regex.Match(endpoint.Address, @"grpc:\\/\\/([^:]+):(\d+)");
                    return Tuple.Create(match.Groups[1].Value, int.Parse(match.Groups[2].Value));
                }
            }
        }

        private static async Task<List<Tuple<string, int>>> FindAllEndpoints()
        {
            var result = new List<Tuple<string, int>>();
            using (var fabricClient = new FabricClient())
            {
                var qm = fabricClient.QueryManager;
                var app = (await qm.GetApplicationListAsync())
                    .First(a => a.ApplicationTypeName == VegaKvsAppTypeName);
                var service = (await qm.GetServiceListAsync(app.ApplicationName))
                    .First(svc => svc.ServiceTypeName == VegaKvsServiceTypeName);
                var endpoints = (await fabricClient.ServiceManager.ResolveServicePartitionAsync(service.ServiceName)).Endpoints;

                foreach (var endpoint in endpoints)
                {
                    // endpoint address looks like: {"Endpoints":{"VegaKvsGrpc":"grpc:\/\/zy.redmond.corp.microsoft.com:64186"}}
                    var match = Regex.Match(endpoint.Address, @"grpc:\\/\\/([^:]+):(\d+)");
                    result.Add(Tuple.Create(match.Groups[1].Value, int.Parse(match.Groups[2].Value)));
                }
            }

            return result;
        }

        private static void PingPongTest(int threadCount, Tuple<string, int> endpoint)
        {
            ResetCounts();
            var rate = TestFlowAsync(
                "Ping-Pong Test",
                endpoint,
                threadCount,
                async (client, cancellationToken, threadId) =>
                {
                    var clock = Stopwatch.StartNew();
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            var tasks = Enumerable.Range(0, AsyncTaskCount)
                                .Select(t => Task.Run(async () => await client.EchoAsync(new VegaKvs.Proto.KeyRequest { Key = "hello" })))
                                .ToArray();

                            await Task.WhenAll(tasks);

                            Interlocked.Add(ref totalDataCount, tasks.Length);
                        }
                        catch (Exception ex)
                        {
                            Log($"Failed to call echo async: {ex.Message}");
                        }
                    }
                },
                30)
                .GetAwaiter().GetResult();
            Log($"Ping-Pong test rate: {rate.Item1:G4} /sec");
        }

        private static void CreateTest(int threadCount, Tuple<string, int> endpoint)
        {
            ResetCounts();
            var rate = TestFlowAsync(
                "Create Test",
                endpoint,
                threadCount,
                (client, cancellationToken, threadId) =>
                {
                    int taskCount = 0;
                    var clock = Stopwatch.StartNew();

                    Random rnd = new Random();
                    var data = new byte[1024];
                    rnd.NextBytes(data);

                    var nodeValue = ByteString.CopyFrom(data);
                    long sequenceNum = 0;

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        SpinWait.SpinUntil(() => taskCount < AsyncTaskCount || cancellationToken.IsCancellationRequested);
                        var task = Task.Run(async () => await client.CreateAsync(new VegaKvs.Proto.Node
                        {
                            Key = $"Thread{threadId}-{Interlocked.Increment(ref sequenceNum)}",
                            Value = nodeValue,
                        }))
                        .ContinueWith(
                            t =>
                            {
                                Interlocked.Decrement(ref taskCount);
                                if (t.Result.Succeeded)
                                {
                                    Interlocked.Increment(ref totalDataCount);
                                }
                                else
                                {
                                    Interlocked.Increment(ref totalFailures);
                                    Log("failed to create");
                                }
                            });

                        Interlocked.Increment(ref taskCount);
                    }

                    SpinWait.SpinUntil(() => taskCount == 0);
                    return Task.FromResult(0);
                },
                120)
                .GetAwaiter().GetResult();
            Log($"Create test rate: {rate.Item1:G4} /sec");
        }

        private static void ReadTest(int threadCount, Tuple<string, int> endpoint)
        {
            ResetCounts();
            var rate = TestFlowAsync(
                "Read Test",
                endpoint,
                threadCount,
                (client, cancellationToken, threadId) =>
                {
                    int taskCount = 0;
                    long sequenceNum = 0;

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        SpinWait.SpinUntil(() => taskCount < AsyncTaskCount || cancellationToken.IsCancellationRequested);
                        var task = Task.Run(async () => await client.ReadAsync(new VegaKvs.Proto.KeyRequest
                        {
                            Key = $"Thread{threadId}-{Interlocked.Increment(ref sequenceNum)}",
                        }))
                        .ContinueWith(
                            t =>
                            {
                                Interlocked.Decrement(ref taskCount);
                                if (t.Result.Succeeded)
                                {
                                    Interlocked.Increment(ref totalDataCount);
                                }
                                else
                                {
                                    Interlocked.Increment(ref totalFailures);
                                }
                            });

                        Interlocked.Increment(ref taskCount);
                    }

                    SpinWait.SpinUntil(() => taskCount == 0);

                    return Task.FromResult(0);
                },
                30)
                .GetAwaiter().GetResult();
            Log($"Read test rate: {rate.Item1:G4} /sec");
        }

        private static async Task ReadAllReplica(int threadCount, List<Tuple<string, int>> endpoints)
        {
            ResetCounts();
            float testRunTime = 30.0F;
            List<Task> tasks = new List<Task>();
            List<long> dataProcessed = new List<long>();
            foreach (var endpoint in endpoints)
            {
                tasks.Add(Task.Run(async () =>
                {
                    var result = await TestFlowAsync(
                        "Read all replica Test",
                        endpoint,
                        threadCount,
                        (client, cancellationToken, threadId) =>
                        {
                            int taskCount = 0;
                            long sequenceNum = 0;

                            while (!cancellationToken.IsCancellationRequested)
                            {
                                SpinWait.SpinUntil(() => taskCount < AsyncTaskCount || cancellationToken.IsCancellationRequested);
                                var task = Task.Run(async () => await client.ReadAsync(new VegaKvs.Proto.KeyRequest
                                {
                                    Key = $"Thread{threadId}-{Interlocked.Increment(ref sequenceNum)}",
                                }))
                                .ContinueWith(
                                    t =>
                                    {
                                        Interlocked.Decrement(ref taskCount);
                                        if (t.Result.Succeeded)
                                        {
                                            Interlocked.Increment(ref totalDataCount);
                                        }
                                        else
                                        {
                                            Interlocked.Increment(ref totalFailures);
                                        }
                                    });

                                Interlocked.Increment(ref taskCount);
                            }

                            SpinWait.SpinUntil(() => taskCount == 0);

                            return Task.FromResult(0);
                        },
                        Convert.ToInt32(testRunTime));

                    dataProcessed.Add(result.Item2);
                }));
            }

            await Task.WhenAll(tasks);

            Log($"total rate is: {totalDataCount / testRunTime:G4} /sec");
        }

        private static async Task ReadAllReplicaStream(int threadCount, List<Tuple<string, int>> endpoints)
        {
            ResetCounts();
            float testRunTime = 30.0F;
            List<Task> tasks = new List<Task>();
            List<long> dataProcessed = new List<long>();
            foreach (var endpoint in endpoints)
            {
                tasks.Add(Task.Run(async () =>
                {
                    var rate = await TestFlowAsync(
                        "Read all replica Test",
                        endpoint,
                        threadCount,
                        async (client, cancellationToken, threadId) =>
                        {
                            long sequenceNum = 0;

                            using (var call = client.ReadStream())
                            {
                                var responseTask = Task.Run(async () =>
                                {
                                    while (await call.ResponseStream.MoveNext())
                                    {
                                        var result = call.ResponseStream.Current;
                                        if (result.Succeeded)
                                        {
                                            Interlocked.Increment(ref totalDataCount);
                                        }
                                        else
                                        {
                                            Interlocked.Increment(ref totalFailures);
                                        }
                                    }
                                });

                                while (!cancellationToken.IsCancellationRequested)
                                {
                                    await call.RequestStream.WriteAsync(new VegaKvs.Proto.KeyRequest
                                    {
                                        Key = $"Thread{threadId}-{Interlocked.Increment(ref sequenceNum)}",
                                    });
                                }

                                await call.RequestStream.CompleteAsync();
                                await responseTask;
                            }
                        },
                        Convert.ToInt32(testRunTime));

                    dataProcessed.Add(rate.Item2);
                }));
            }

            await Task.WhenAll(tasks);

            Log($"total rate is: {totalDataCount / testRunTime:G4} /sec");
        }

        private static void ReadStreamTest(int threadCount, Tuple<string, int> endpoint)
        {
            ResetCounts();
            var rate = TestFlowAsync(
                "ReadStream Test",
                endpoint,
                threadCount,
                async (client, cancellationToken, threadId) =>
                {
                    long sequenceNum = 0;

                    using (var call = client.ReadStream())
                    {
                        var responseTask = Task.Run(async () =>
                        {
                            while (await call.ResponseStream.MoveNext())
                            {
                                var result = call.ResponseStream.Current;
                                if (result.Succeeded)
                                {
                                    Interlocked.Increment(ref totalDataCount);
                                }
                                else
                                {
                                    Interlocked.Increment(ref totalFailures);
                                }
                            }
                        });

                        while (!cancellationToken.IsCancellationRequested)
                        {
                            await call.RequestStream.WriteAsync(new VegaKvs.Proto.KeyRequest
                            {
                                Key = $"Thread{threadId}-{Interlocked.Increment(ref sequenceNum)}",
                            });
                        }

                        await call.RequestStream.CompleteAsync();
                        await responseTask;
                    }
                },
                30)
                .GetAwaiter().GetResult();
            Log($"ReadStream test rate: {rate.Item1:G4} /sec");
        }

        private static void MixedRWTest(int threadCount, Tuple<string, int> endpoint, double ratio)
        {
            ResetCounts();
            long totalReadDataCount = 0;
            long totalWriteDataCount = 0;

            var rate = TestFlowAsync(
                "Mixed RW Test",
                endpoint,
                threadCount,
                (client, cancellationToken, threadId) =>
                {
                    int taskCount = 0;

                    Random rnd = new Random();
                    var data = new byte[1024];
                    rnd.NextBytes(data);

                    var nodeValue = ByteString.CopyFrom(data);
                    long readSequenceNum = 0;
                    long createSequenceNum = 0;

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        SpinWait.SpinUntil(() => taskCount < AsyncTaskCount || cancellationToken.IsCancellationRequested);

                        if (rnd.NextDouble() > ratio)
                        {
                            var task = Task.Run(async () => await client.ReadAsync(new VegaKvs.Proto.KeyRequest
                            {
                                Key = $"Thread{threadId}-{Interlocked.Increment(ref readSequenceNum)}",
                            }))
                            .ContinueWith(
                                t =>
                                {
                                    Interlocked.Decrement(ref taskCount);
                                    Interlocked.Increment(ref totalReadDataCount);
                                    if (t.Result.Succeeded)
                                    {
                                        Interlocked.Increment(ref totalDataCount);
                                    }
                                    else
                                    {
                                        Interlocked.Increment(ref totalFailures);
                                        Log("failed to read");
                                    }
                                });
                        }
                        else
                        {
                            var task = Task.Run(async () => await client.CreateAsync(new VegaKvs.Proto.Node
                            {
                                Key = $"MixedRW-Thread{threadId}-{Interlocked.Increment(ref createSequenceNum)}",
                                Value = nodeValue,
                            }))
                            .ContinueWith(
                                t =>
                                {
                                    Interlocked.Decrement(ref taskCount);
                                    Interlocked.Increment(ref totalWriteDataCount);
                                    if (t.Result.Succeeded)
                                    {
                                        Interlocked.Increment(ref totalDataCount);
                                    }
                                    else
                                    {
                                        Interlocked.Increment(ref totalFailures);
                                        Log("failed to create");
                                    }
                                });
                        }

                        Interlocked.Increment(ref taskCount);
                    }

                    SpinWait.SpinUntil(() => taskCount == 0);

                    return Task.FromResult(0);
                },
                20)
                .GetAwaiter().GetResult();
            Log($"Mixed RW test rate: {rate:G4} /sec with ratio {(decimal)totalWriteDataCount / totalReadDataCount}");
        }

        private static void DeleteTest(int threadCount, Tuple<string, int> endpoint)
        {
            ResetCounts();
            var rate = TestFlowAsync(
                "delete Test",
                endpoint,
                threadCount,
                (client, cancellationToken, threadId) =>
                {
                    int taskCount = 0;
                    long sequenceNum = 0;

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        SpinWait.SpinUntil(() => taskCount < AsyncTaskCount || cancellationToken.IsCancellationRequested);
                        var task = Task.Run(async () => await client.DeleteAsync(new VegaKvs.Proto.KeyRequest
                        {
                            Key = $"Thread{threadId}-{Interlocked.Increment(ref sequenceNum)}",
                        }))
                        .ContinueWith(
                            t =>
                            {
                                Interlocked.Decrement(ref taskCount);
                                if (t.Result.Succeeded)
                                {
                                    Interlocked.Increment(ref totalDataCount);
                                }
                                else
                                {
                                    Interlocked.Increment(ref totalFailures);
                                    Log($"Failed to delete");
                                }
                            });

                        Interlocked.Increment(ref taskCount);
                    }

                    SpinWait.SpinUntil(() => taskCount == 0);

                    return Task.FromResult(0);
                },
                30)
                .GetAwaiter().GetResult();
            Log($"Delete test rate: {rate:G4} /sec");
        }

        private static async Task<Tuple<double, long>> TestFlowAsync(
            string testTitle,
            Tuple<string, int> endpoint,
            int threadCount,
            Func<KeyValueStoreClient, CancellationToken, int, Task> workload,
            int durationInSeconds)
        {
            var cancellation = new CancellationTokenSource();

            Log($"Starting test {testTitle} in {threadCount} threads");
            var clients = Enumerable.Range(0, threadCount).Select(x => new KeyValueStoreClient(new Channel(endpoint.Item1, endpoint.Item2, ChannelCredentials.Insecure))).ToArray();

            var lastCount = Interlocked.Read(ref totalDataCount);
            var lastSize = Interlocked.Read(ref totalDataSize);

            var threads = StartMultipleThreads(
                threadCount,
                (object n) => workload(clients[(int)n], cancellation.Token, (int)n).GetAwaiter().GetResult());

            var initialCount = lastCount;
            var initialSize = lastSize;
            var stopwatch = Stopwatch.StartNew();

            for (int i = 0; i < durationInSeconds; i++)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));

                long size = Interlocked.Read(ref totalDataSize);
                long delta = size - lastSize;

                long count = Interlocked.Read(ref totalDataCount);
                long deltaCount = count - lastCount;

                Log($"{DateTime.Now} - {deltaCount} - {delta}");

                lastSize = size;
                lastCount = count;
            }

            stopwatch.Stop();
            var processedCount = Interlocked.Read(ref totalDataCount) - initialCount;
            var processedSize = Interlocked.Read(ref totalDataSize) - initialSize;
            var rate = processedCount / stopwatch.Elapsed.TotalSeconds;

            Log($"Stopping test {testTitle}. Data processed {processedSize} bytes in {processedCount} ops. Failures = {totalFailures}");
            cancellation.Cancel();

            foreach (var thread in threads)
            {
                thread.Join();
            }

            Log($"Stopped {testTitle}.");

            return Tuple.Create(rate, processedCount);
        }

        private static void ResetCounts()
        {
            totalDataCount = totalDataSize = totalFailures = 0;
        }

        private static Thread[] StartMultipleThreads(int threadCount, ParameterizedThreadStart action)
        {
            var threads = new List<Thread>();
            for (int i = 0; i < threadCount; i++)
            {
                threads.Add(new Thread(action));
            }

            for (int i = 0; i < threadCount; i++)
            {
                threads[i].Start(i);
            }

            return threads.ToArray();
        }
    }
}
