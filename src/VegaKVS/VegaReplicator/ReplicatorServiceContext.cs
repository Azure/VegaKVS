// <copyright file="ReplicatorServiceContext.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Text;
    using System.Threading.Tasks;

    /// <summary>
    /// Commonly used objects among state providers and service objects.
    /// </summary>
    public sealed class ReplicatorServiceContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ReplicatorServiceContext"/> class.
        /// </summary>
        /// <param name="serviceContext">Stateful service context.</param>
        public ReplicatorServiceContext(StatefulServiceContext serviceContext)
        {
            this.ServiceContext = serviceContext;
        }

        /// <summary>
        /// Gets the stateful service context.
        /// </summary>
        public StatefulServiceContext ServiceContext { get; private set; }

        /// <summary>
        /// Gets or sets the replicator object.
        /// </summary>
        public FabricReplicator Replicator { get; set; }

        /// <summary>
        /// Gets or sets the callback function to process the replication operation.
        /// </summary>
        public Func<IOperation, Task> ProcessReplicatedOperation { get; set; }
    }
}
