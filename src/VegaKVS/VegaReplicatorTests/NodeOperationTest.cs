// <copyright file="NodeOperationTest.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// NodeOperation test cases.
    /// </summary>
    [TestClass]
    public class NodeOperationTest
    {
        /// <summary>
        /// Serialization and deserialization.
        /// </summary>
        [TestMethod]
        public void Serialization()
        {
            var nodeOp1 = new NodeOperation(
                NodeOperation.OperationKind.Create,
                10,
                "A",
                new ArraySegment<byte>(new byte[] { 1, 2, 3, }));
            var nodeOp2 = new NodeOperation(
                NodeOperation.OperationKind.Update,
                11,
                "BB",
                new ArraySegment<byte>(new byte[] { 2, 3, 4, 5, }));
            var nodeOp3 = new NodeOperation(
                NodeOperation.OperationKind.Delete,
                12,
                "CCC",
                default);

            var opData = NodeOperation.GetOperationData(new[] { nodeOp1, nodeOp2, nodeOp3, });
            Assert.AreEqual(1, opData.Count);

            var ops = NodeOperation.GetNodeOperationFromBytes(opData.First()).ToArray();
            Assert.AreEqual(3, ops.Length);

            Assert.AreEqual(NodeOperation.OperationKind.Create, ops[0].Operation);
            Assert.AreEqual("A", ops[0].Key);
            Assert.AreEqual(3, ops[0].Value.Count);

            Assert.AreEqual(NodeOperation.OperationKind.Update, ops[1].Operation);
            Assert.AreEqual("BB", ops[1].Key);
            Assert.AreEqual(4, ops[1].Value.Count);

            Assert.AreEqual(NodeOperation.OperationKind.Delete, ops[2].Operation);
            Assert.AreEqual("CCC", ops[2].Key);
            Assert.AreEqual(0, ops[2].Value.Count);
            Assert.IsNull(ops[2].Value.Array);

            Assert.IsNotNull(ops[0].ToString());
            Assert.IsNotNull(ops[1].ToString());
            Assert.IsNotNull(ops[2].ToString());
        }
    }
}
