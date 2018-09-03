// <copyright file="NodeOperation.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved.
// </copyright>

namespace Microsoft.Vega
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.IO;
    using System.Text;

    /// <summary>
    /// Operation on individual node, or key value pair in the store.
    /// </summary>
    public sealed class NodeOperation
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NodeOperation"/> class.
        /// </summary>
        /// <param name="operation">Operation.</param>
        /// <param name="sequenceNumber">Sequence number.</param>
        /// <param name="key">Key of the node.</param>
        /// <param name="value">Value of the node.</param>
        public NodeOperation(OperationKind operation, long sequenceNumber, string key, ArraySegment<byte> value)
        {
            this.Operation = operation;
            this.SequenceNumber = sequenceNumber;
            this.Key = key;
            this.Value = value;
        }

        /// <summary>
        /// Kind of operation on nodes.
        /// </summary>
        public enum OperationKind : byte
        {
            /// <summary>
            /// No operation, This is to tell secondaries the current committed sequence number.
            /// </summary>
            Nop = 0,

            /// <summary>
            /// Create a new node
            /// </summary>
            Create,

            /// <summary>
            /// Update an exiting node
            /// </summary>
            Update,

            /// <summary>
            /// Delete an existing node
            /// </summary>
            Delete,
        }

        /// <summary>
        /// Gets the operation on the node.
        /// </summary>
        public OperationKind Operation { get; private set; }

        /// <summary>
        /// Gets the sequence number of the operation.
        /// </summary>
        public long SequenceNumber { get; private set; }

        /// <summary>
        /// Gets the key of the node.
        /// </summary>
        public string Key { get; private set; }

        /// <summary>
        /// Gets the value of the node.
        /// </summary>
        public ArraySegment<byte> Value { get; private set; }

        /// <summary>
        /// Deserializes the specified byte array and gets <see cref="NodeOperation"/> object.
        /// </summary>
        /// <param name="bytes">byte array to deserialize.</param>
        /// <returns>Node operation object collection.</returns>
        public static IEnumerable<NodeOperation> GetNodeOperationFromBytes(ArraySegment<byte> bytes)
        {
            MemoryStream memoryStream = null;
            BinaryReader binaryReader = null;
            try
            {
                memoryStream = new MemoryStream(bytes.Array, bytes.Offset, bytes.Count);
                binaryReader = new BinaryReader(memoryStream);

                while (memoryStream.Position < bytes.Count)
                {
                    var op = (OperationKind)binaryReader.ReadByte();
                    var sn = binaryReader.ReadInt64();
                    var key = default(string);
                    var data = default(ArraySegment<byte>);

                    if (op != OperationKind.Nop)
                    {
                        key = binaryReader.ReadString();
                        var valueLength = binaryReader.ReadInt32();

                        // Directly take segment from bytes to avoid byte array allocation and copy
                        var pos = (int)memoryStream.Position;
                        data = valueLength == 0
                            ? default
                            : new ArraySegment<byte>(bytes.Array, pos + bytes.Offset, valueLength);
                        memoryStream.Position = pos + valueLength;
                    }

                    yield return new NodeOperation(op, sn, key, data);
                }
            }
            finally
            {
                if (binaryReader != null)
                {
                    binaryReader.Dispose();
                    memoryStream = null;
                }

                if (memoryStream != null)
                {
                    memoryStream.Dispose();
                }
            }
        }

        /// <summary>
        /// Serializes and gets the specified collection of operation data for replication.
        /// </summary>
        /// <param name="nodeOperations">Collection of node operations.</param>
        /// <returns>Operation data object.</returns>
        public static OperationData GetOperationData(IEnumerable<NodeOperation> nodeOperations)
        {
            MemoryStream memoryStream = null;
            BinaryWriter binaryWriter = null;
            try
            {
                memoryStream = new MemoryStream();
                binaryWriter = new BinaryWriter(memoryStream);

                foreach (var op in nodeOperations)
                {
                    WriteBinary(binaryWriter, op);
                }

                ArraySegment<byte> array;
                if (memoryStream.TryGetBuffer(out array))
                {
                    return new OperationData(array);
                }
                else
                {
                    return new OperationData(memoryStream.ToArray());
                }
            }
            finally
            {
                if (binaryWriter != null)
                {
                    binaryWriter.Dispose();

                    // Mark it as disposed
                    memoryStream = null;
                }

                if (memoryStream != null)
                {
                    memoryStream.Dispose();
                }
            }
        }

        /// <summary>
        /// Serializes and gets the operation data for replication.
        /// </summary>
        /// <returns>Operation data object.</returns>
        public OperationData GetOperationData()
        {
            // Maximum size of the buffer. In reality string is encoded, and size of string is 7-bit encoded.
            int capacity = (this.Key.Length * 2) + this.Value.Count + (sizeof(int) * 3) + sizeof(OperationKind);

            MemoryStream memoryStream = null;
            BinaryWriter binaryWriter = null;
            try
            {
                memoryStream = new MemoryStream(capacity);
                binaryWriter = new BinaryWriter(memoryStream);
                WriteBinary(binaryWriter, this);

                ArraySegment<byte> array;
                if (memoryStream.TryGetBuffer(out array))
                {
                    return new OperationData(array);
                }
                else
                {
                    return new OperationData(memoryStream.ToArray());
                }
            }
            finally
            {
                if (binaryWriter != null)
                {
                    binaryWriter.Dispose();

                    // Mark it as disposed
                    memoryStream = null;
                }

                if (memoryStream != null)
                {
                    memoryStream.Dispose();
                }
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return string.Join(
                "/",
                this.SequenceNumber,
                this.Operation,
                this.Key,
                this.Value.Count == 0 ? "-" : string.Join(",", this.Value));
        }

        private static void WriteBinary(BinaryWriter binaryWriter, NodeOperation nodeOperation)
        {
            binaryWriter.Write((byte)nodeOperation.Operation);
            binaryWriter.Write(nodeOperation.SequenceNumber);

            // Nop is only for broadcasting sequence number, no key/value data.
            if (nodeOperation.Operation != OperationKind.Nop)
            {
                binaryWriter.Write(nodeOperation.Key);
                binaryWriter.Write(nodeOperation.Value.Count);

                if (nodeOperation.Value.Count > 0)
                {
                    binaryWriter.Write(nodeOperation.Value.Array, nodeOperation.Value.Offset, nodeOperation.Value.Count);
                }
            }
        }
    }
}
