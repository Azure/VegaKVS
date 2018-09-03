// <copyright file="ServiceEventSource.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Tracing;
    using System.Fabric;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Services.Runtime;

#pragma warning disable SA1600 // element should be documented.

    /// <summary>
    /// Event source for logging and tracing.
    /// </summary>
    [EventSource(Name = "Microsoft-VegaKvsService")]
    public sealed class ServiceEventSource : EventSource
    {
        /// <summary>
        /// Singleton instance of the event source.
        /// </summary>
        public static readonly ServiceEventSource Current = new ServiceEventSource();

        private const int MessageEventId = 1;
        private const int ServiceMessageEventId = 2;
        private const int ServiceTypeRegisteredEventId = 3;
        private const int ServiceHostInitializationFailedEventId = 4;

        // A pair of events sharing the same name prefix with a "Start"/"Stop" suffix implicitly marks boundaries of an event tracing activity.
        // These activities can be automatically picked up by debugging and profiling tools, which can compute their execution time, child activities,
        // and other statistics.
        private const int ServiceRequestStartEventId = 5;
        private const int ServiceRequestStopEventId = 6;
        private const int DebugEventId = 7;

        // Instance constructor is private to enforce singleton semantics
        private ServiceEventSource()
            : base()
        {
        }

        [NonEvent]
        public void Message(string message, params object[] args)
        {
            string finalMessage = string.Format(message, args);
            this.Message(finalMessage);
        }

        [Event(MessageEventId, Level = EventLevel.Informational, Message="{0}")]
        public void Message(string message)
        {
            this.WriteEvent(MessageEventId, message);
        }

        [NonEvent]
        public void ServiceMessage(StatefulServiceContext serviceContext, string message, params object[] args)
        {
            string finalMessage = string.Format(message, args);
            this.ServiceMessage(
                serviceContext.ServiceName.ToString(),
                serviceContext.ServiceTypeName,
                serviceContext.ReplicaId,
                serviceContext.PartitionId,
                serviceContext.CodePackageActivationContext.ApplicationName,
                serviceContext.CodePackageActivationContext.ApplicationTypeName,
                serviceContext.NodeContext.NodeName,
                finalMessage);
        }

        [Event(ServiceTypeRegisteredEventId, Level = EventLevel.Informational, Message = "Service host process {0} registered service type {1}", Keywords = Keywords.ServiceInitialization)]
        public void ServiceTypeRegistered(int hostProcessId, string serviceType)
        {
            this.WriteEvent(ServiceTypeRegisteredEventId, hostProcessId, serviceType);
        }

        [Event(ServiceHostInitializationFailedEventId, Level = EventLevel.Error, Message = "Service host initialization failed", Keywords = Keywords.ServiceInitialization)]
        public void ServiceHostInitializationFailed(string exception)
        {
            this.WriteEvent(ServiceHostInitializationFailedEventId, exception);
        }

        [Event(ServiceRequestStartEventId, Level = EventLevel.Informational, Message = "Service request '{0}' started", Keywords = Keywords.Requests)]
        public void ServiceRequestStart(string requestTypeName)
        {
            this.WriteEvent(ServiceRequestStartEventId, requestTypeName);
        }

        [Event(ServiceRequestStopEventId, Level = EventLevel.Informational, Message = "Service request '{0}' finished", Keywords = Keywords.Requests)]
        public void ServiceRequestStop(string requestTypeName, string exception = "")
        {
            this.WriteEvent(ServiceRequestStopEventId, requestTypeName, exception);
        }

        [Event(DebugEventId, Level = EventLevel.Verbose, Message = "{0}")]
        public void Debug(string message)
        {
            this.WriteEvent(DebugEventId, message);
        }

        private static int SizeInBytes(string s)
        {
            if (s == null)
            {
                return 0;
            }
            else
            {
                return (s.Length + 1) * sizeof(char);
            }
        }

        [Event(ServiceMessageEventId, Level = EventLevel.Informational, Message = "{7}")]
        private unsafe void ServiceMessage(
            string serviceName,
            string serviceTypeName,
            long replicaOrInstanceId,
            Guid partitionId,
            string applicationName,
            string applicationTypeName,
            string nodeName,
            string message)
        {
            const int numArgs = 8;
            fixed (char* pServiceName = serviceName, pServiceTypeName = serviceTypeName, pApplicationName = applicationName, pApplicationTypeName = applicationTypeName, pNodeName = nodeName, pMessage = message)
            {
                EventData* eventData = stackalloc EventData[numArgs];
                eventData[0] = new EventData { DataPointer = (IntPtr)pServiceName, Size = SizeInBytes(serviceName) };
                eventData[1] = new EventData { DataPointer = (IntPtr)pServiceTypeName, Size = SizeInBytes(serviceTypeName) };
                eventData[2] = new EventData { DataPointer = (IntPtr)(&replicaOrInstanceId), Size = sizeof(long) };
                eventData[3] = new EventData { DataPointer = (IntPtr)(&partitionId), Size = sizeof(Guid) };
                eventData[4] = new EventData { DataPointer = (IntPtr)pApplicationName, Size = SizeInBytes(applicationName) };
                eventData[5] = new EventData { DataPointer = (IntPtr)pApplicationTypeName, Size = SizeInBytes(applicationTypeName) };
                eventData[6] = new EventData { DataPointer = (IntPtr)pNodeName, Size = SizeInBytes(nodeName) };
                eventData[7] = new EventData { DataPointer = (IntPtr)pMessage, Size = SizeInBytes(message) };

                this.WriteEventCore(ServiceMessageEventId, numArgs, eventData);
            }
        }

        /// <summary>
        /// Event keywords can be used to categorize events.
        /// Each keyword is a bit flag. A single event can be associated with multiple keywords (via EventAttribute.Keywords property).
        /// Keywords must be defined as a public class named 'Keywords' inside EventSource that uses them.
        /// </summary>
        public static class Keywords
        {
            public const EventKeywords Requests = (EventKeywords)0x1L;
            public const EventKeywords ServiceInitialization = (EventKeywords)0x2L;
        }
    }
}
