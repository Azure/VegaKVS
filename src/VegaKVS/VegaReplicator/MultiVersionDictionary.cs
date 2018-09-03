// <copyright file="MultiVersionDictionary.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    /// <summary>
    /// A dictionary where the value is versioned using 64-bit integer, and key is a string.
    /// </summary>
    /// <typeparam name="TValue">Type of values stored in the dictionary.</typeparam>
    /// <remarks>
    /// TValue must be a reference type, such as byte array. We use null to mark the correspondong value as removed.
    /// </remarks>
    public sealed class MultiVersionDictionary<TValue>
        where TValue : class
    {
        private static readonly TValue EmptyMarker = null;
        private readonly ConcurrentDictionary<string, LinkedList<VersionValue>> dataStore;

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiVersionDictionary{TValue}"/> class.
        /// </summary>
        public MultiVersionDictionary()
        {
            this.dataStore = new ConcurrentDictionary<string, LinkedList<VersionValue>>(StringComparer.Ordinal);
        }

        /// <summary>
        /// Adds a new item or updates an existing item.
        /// </summary>
        /// <param name="key">Key of the item to be added or updated.</param>
        /// <param name="sn">Sequence number of this key-value pair.</param>
        /// <param name="value">Value of the item to be added or updated.</param>
        /// <returns>Whether the request is succeeded or not.</returns>
        public bool AddOrUpdate(string key, long sn, TValue value)
        {
            // Null value is explicity disallowed since it is used as a removed marker.
            if (value == EmptyMarker)
            {
                return false;
            }

            bool succeeded = true;
            this.dataStore.AddOrUpdate(
                key,
                new LinkedList<VersionValue>(new[] { new VersionValue(sn, value), }),
                (k, v) =>
                {
                    // If the list is empty, we leave it intact so it can be safely deleted.
                    if (v.Count == 0)
                    {
                        return new LinkedList<VersionValue>(new[] { new VersionValue(sn, value), });
                    }
                    else
                    {
                        // Otherwise we append. Note other thread may be calling TryGetValue, GetSnapshot, or
                        // RemoveOldVersionValues, but not TryRemove because all write operations are serialized.
                        lock (((System.Collections.ICollection)v).SyncRoot)
                        {
                            if (v.Last.Value.Version <= sn)
                            {
                                v.AddLast(new LinkedListNode<VersionValue>(new VersionValue(sn, value)));
                                return v;
                            }
                            else
                            {
                                succeeded = false;
                                return v;
                            }
                        }
                    }
                });

            return succeeded;
        }

        /// <summary>
        /// Tries to get the value for the given key and sequence number.
        /// </summary>
        /// <param name="key">Key of the item to get.</param>
        /// <param name="sn">Sequence number.</param>
        /// <param name="value">Value got on output.</param>
        /// <returns>Whether the request is succeeded or not.</returns>
        public bool TryGetValue(string key, long sn, out TValue value)
        {
            LinkedList<VersionValue> vals;
            if (this.dataStore.TryGetValue(key, out vals))
            {
                lock (((System.Collections.ICollection)vals).SyncRoot)
                {
                    var p = vals.First;

                    do
                    {
                        // Linked list is circular
                        if (p.Value.Version <= sn && (p.Next == null || (p.Next != null && p.Next.Value.Version > sn)))
                        {
                            value = p.Value.Value;
                            return value != EmptyMarker;
                        }

                        p = p.Next;
                    }
                    while (p != null);
                }
            }

            value = EmptyMarker;
            return false;
        }

        /// <summary>
        /// Marks the key as removed, without actually deleting the item.
        /// </summary>
        /// <param name="key">Key of the item to get.</param>
        /// <param name="sn">Sequence number.</param>
        /// <returns>Whether the request is succeeded or not.</returns>
        public bool TryRemove(string key, long sn)
        {
            LinkedList<VersionValue> vals;
            if (this.dataStore.TryGetValue(key, out vals))
            {
                lock (((System.Collections.ICollection)vals).SyncRoot)
                {
                    if (vals.Last.Value.Version <= sn)
                    {
                        vals.AddLast(new VersionValue(sn, EmptyMarker));
                        return true;
                    }
                }
            }

            return false;
        }

        /// <summary>
        /// Gets a snapshot up to the specified sequence number.
        /// </summary>
        /// <param name="sn">Maximum sequence number.</param>
        /// <returns>Collection of key value pairs and their sequence numbers.</returns>
        public IEnumerable<Tuple<long, string, TValue>> GetSnapshot(long sn)
        {
            foreach (var node in this.dataStore)
            {
                var key = node.Key;

                // TODO: lock?
                var p = node.Value.First;

                do
                {
                    // Linked list is circular
                    if (p.Value.Version <= sn && (p.Next == null || (p.Next != null && p.Next.Value.Version > sn)))
                    {
                        var value = p.Value.Value;
                        if (value != EmptyMarker)
                        {
                            yield return Tuple.Create(p.Value.Version, key, value);
                        }
                    }

                    p = p.Next;
                }
                while (p != null);
            }
        }

        /// <summary>
        /// Performs the garbage collection after previous snapshot is stored to disk.
        /// </summary>
        /// <param name="sn">Maximum sequence number of all stored data.</param>
        /// <returns>Number of key-value pairs removed.</returns>
        public int RemoveOldVersionValues(long sn)
        {
            foreach (var node in this.dataStore)
            {
                var key = node.Key;
                var vals = node.Value;
                lock (((System.Collections.ICollection)vals).SyncRoot)
                {
                    var p = vals.First;

                    // If the first node is old based on the version/sn, and it has subsequent change, delete it.
                    // The end result is that, either all remaining nodes are newer, or there is only one remaining
                    // node which is older.
                    while (vals.First.Next != null && vals.First.Value.Version <= sn)
                    {
                        vals.RemoveFirst();
                    }

                    // If the first node is remove and it's older, then it must be the only one remaining.
                    if (vals.First.Value.Value == EmptyMarker)
                    {
                        vals.RemoveFirst();
                    }

                    // At this point, the linked list may be the following state:
                    // 1. empty -- last operation is delete.
                    // 2. one node, and it is the last operation which may be newer or older than sn.
                    // 3. multiple nodes, and all of them are more recent than sn.
                }
            }

            // Without locking the entire dataStore, it is unsafe to remove empty items (entirely removed key-value
            // pairs) using ConcurrentDictionary methods. But ICollection<>.Remove verifies before remove, and new
            // items are never appended to empty linked list (see AddOrUpdate method), so we can delete safely.
            var dataStoreCollection = (ICollection<KeyValuePair<string, LinkedList<VersionValue>>>)this.dataStore;

            // Materialize the list since we cannot delete while enumerating.
            var itemsToRemove = dataStoreCollection.Where(kvp => kvp.Value.Count == 0).ToArray();

            // If Remove returns false, the linked list is just changed and we just ignore the error.
            var itemsRemoved = itemsToRemove.Count(it => dataStoreCollection.Remove(it));
            return itemsRemoved;
        }

        private readonly struct VersionValue : IEquatable<VersionValue>
        {
            public VersionValue(long ver, TValue val)
            {
                this.Version = ver;
                this.Value = val;
            }

            public long Version { get; }

            public TValue Value { get; }

            // Value is ignored on purpose.
            public bool Equals(VersionValue other) => this.Version == other.Version;
        }
    }
}
