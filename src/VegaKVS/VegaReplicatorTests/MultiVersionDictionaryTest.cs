// <copyright file="MultiVersionDictionaryTest.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// MultiVersionDictionary class unit tests.
    /// </summary>
    [TestClass]
    public class MultiVersionDictionaryTest
    {
        /// <summary>
        /// Basic life cycle test case.
        /// </summary>
        [TestMethod]
        public void BasicLifecycle()
        {
            var dict = new MultiVersionDictionary<V>();
            V v;
            V v1 = new V(1);

            Assert.IsFalse(dict.TryGetValue("A", 0, out _));
            Assert.IsFalse(dict.TryRemove("A", 0));

            Assert.IsTrue(dict.AddOrUpdate("A", 1, v1));
            Assert.IsFalse(dict.TryGetValue("A", 0, out _));
            Assert.IsTrue(dict.TryGetValue("A", 1, out v));
            Assert.AreEqual(v1, v);
            Assert.IsTrue(dict.TryGetValue("A", 2, out v));
            Assert.AreEqual(v1, v);

            Assert.IsFalse(dict.TryRemove("A", 0));
            Assert.IsTrue(dict.TryRemove("A", 2));
            Assert.IsFalse(dict.TryGetValue("A", 0, out _));
            Assert.IsTrue(dict.TryGetValue("A", 1, out v));
            Assert.AreEqual(v1, v);
            Assert.IsFalse(dict.TryGetValue("A", 2, out v));
        }

        /// <summary>
        /// Gets snapshot of data store and performs garbage collection.
        /// </summary>
        [TestMethod]
        public void GetSnapshotAndGC()
        {
            var dict = new MultiVersionDictionary<V>();
            dict.AddOrUpdate("a", 1, new V(1));
            dict.AddOrUpdate("b", 2, new V(2));
            dict.AddOrUpdate("c", 3, new V(3));
            dict.AddOrUpdate("a", 4, new V(4));
            dict.AddOrUpdate("a", 5, new V(5));
            dict.TryRemove("b", 6);
            dict.AddOrUpdate("c", 7, new V(7));
            dict.AddOrUpdate("b", 8, new V(8));

            var ss0 = dict.GetSnapshot(0).ToArray();
            Assert.AreEqual(0, ss0.Length);

            var ss1 = dict.GetSnapshot(1).ToArray();
            Assert.AreEqual(1, ss1.Length);
            Assert.AreEqual(1, ss1[0].Item1);
            Assert.AreEqual("a", ss1[0].Item2);

            var ss3 = dict.GetSnapshot(3).ToArray();
            Assert.AreEqual(3, ss3.Length);

            var ss5 = dict.GetSnapshot(5).OrderBy(x => x.Item2).ToArray();
            Assert.AreEqual(3, ss5.Length);

            var ss6 = dict.GetSnapshot(6).OrderBy(x => x.Item2).ToArray();
            Assert.AreEqual(2, ss6.Length);

            var ss8 = dict.GetSnapshot(8).OrderBy(x => x.Item2).ToArray();
            Assert.AreEqual(3, ss8.Length);

            dict.RemoveOldVersionValues(6);
        }

        private class V : IEquatable<V>
        {
            private readonly int val;

            public V(int val) => this.val = val;

            public bool Equals(V other) => this.val == other.val;
        }
    }
}
