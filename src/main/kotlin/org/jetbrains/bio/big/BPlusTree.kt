package org.jetbrains.bio.big

import com.google.common.math.IntMath
import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import org.jetbrains.bio.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.ByteOrder

/**
 * A B+ tree mapping chromosome names to (id, size) pairs.
 *
 * Here `id` is a unique positive integer and size is
 * chromosome length in base pairs. Contrary to the original
 * definition the leaves in this B+ tree aren't linked.
 *
 * See tables 8-11 in Supplementary Data for byte-to-byte details
 * on the B+ header and node formats.
 */
internal class BPlusTree(val header: BPlusTree.Header) {
    /**
     * Recursively goes across tree, calling callback on the leaves.
     */
    @Throws(IOException::class)
    fun traverse(input: RomBuffer): Sequence<BPlusLeaf> {
        return if (header.itemCount == 0) {
            emptySequence()
        } else {
            traverseRecursively(input, header.rootOffset)
        }
    }

    private fun traverseRecursively(input: RomBuffer,
                                    offset: Long): Sequence<BPlusLeaf> {
        assert(input.order == header.order)
        input.position = offset

        val isLeaf = input.readByte() > 0
        input.readByte()  // reserved.
        val childCount = input.readUnsignedShort()

        return if (isLeaf) {
            (0 until childCount)
                    .mapUnboxed { BPlusLeaf.read(input, header.keySize) }
                    .toList().asSequence()
        } else {
            (0 until childCount)
                    .mapUnboxed { BPlusNode.read(input, header.keySize) }
                    .toList().asSequence()
                    .flatMap { traverseRecursively(input, it.childOffset) }
        }
    }

    /**
     * Recursively traverses a B+ tree looking for a leaf corresponding
     * to `query`.
     */
    @Throws(IOException::class)
    fun find(input: RomBuffer, query: String): BPlusLeaf? {
        if (header.itemCount == 0 || query.length > header.keySize) {
            return null
        }

        return findRecursively(input, header.rootOffset, query)
    }

    private tailrec fun findRecursively(input: RomBuffer, blockStart: Long,
                                        query: String): BPlusLeaf? {
        assert(input.order == header.order)
        input.position = blockStart

        val isLeaf = input.readByte() > 0
        input.readByte()  // reserved.
        val childCount = input.readUnsignedShort()

        if (isLeaf) {
            for (i in 0 until childCount) {
                val leaf = BPlusLeaf.read(input, header.keySize)
                if (leaf.key == query) {
                    return leaf
                }
            }

            return null
        } else {
            var node = BPlusNode.read(input, header.keySize)
            // vvv loop from 1 because we've read the first child above.
            for (i in 1 until childCount) {
                val next = BPlusNode.read(input, header.keySize)
                if (query < next.key) {
                    break
                }

                node = next
            }

            return findRecursively(input, node.childOffset, query)
        }
    }

    internal data class Header(val order: ByteOrder,
                               val blockSize: Int, val keySize: Int,
                               val itemCount: Int, val rootOffset: Long) {
        fun write(output: OrderedDataOutput) = with(output) {
            writeInt(MAGIC)
            writeInt(blockSize)
            writeInt(keySize)
            writeInt(Ints.BYTES * 2)  // (ID, Size)
            writeLong(itemCount.toLong())
            writeLong(0L)             // reserved.
        }

        companion object {
            /** Number of bytes used for this header. */
            internal val BYTES = Ints.BYTES * 4 + Longs.BYTES * 2
            /** Magic number used for determining [ByteOrder]. */
            private val MAGIC = 0x78CA8C91

            fun read(input: RomBuffer, offset: Long) = with(input) {
                val expectedOrder = order
                position = offset
                checkHeader(MAGIC)
                check(order == expectedOrder)

                val blockSize = readInt()
                val keySize = readInt()
                val valSize = readInt()
                check(valSize == Ints.BYTES * 2) { "inconsistent value size: $valSize" }

                val itemCount = readLong()
                readLong()  // reserved.
                val rootOffset = position

                Header(order, blockSize, keySize, Ints.checkedCast(itemCount),
                       rootOffset)
            }
        }
    }

    companion object {
        private val LOG = LoggerFactory.getLogger(BPlusTree::class.java)

        internal fun read(input: RomBuffer, offset: Long): BPlusTree {
            return BPlusTree(Header.read(input, offset))
        }

        internal fun write(output: OrderedDataOutput, unsortedItems: List<BPlusLeaf>,
                           blockSize: Int = Math.max(Math.min(unsortedItems.size, 256), 2)) {
            require(blockSize > 1) { "blockSize must be >1" }
            val items = unsortedItems.sortedBy { it.key }
            val keySize = (items.map { it.key.length }.max() ?: 0)
            // ^^^ the +1 is to account for the trailing null.

            val header = Header(output.order, blockSize, keySize, items.size,
                                output.tell() + Header.BYTES)
            header.write(output)

            if (items.isEmpty()) {
                return
            }

            LOG.debug("Creating a B+ tree for ${items.size} items ($blockSize slots/node)")
            writeLevels(output, items, blockSize, keySize)
            writeLeaves(output, items, blockSize, keySize)
            LOG.debug("Saved B+ tree using ${output.tell() - header.rootOffset} bytes")
        }

        /** Writes out final leaf level in a B+ tree. */
        private fun writeLeaves(output: OrderedDataOutput, items: List<BPlusLeaf>,
                                blockSize: Int, keySize: Int) {
            val bytesInLeafSlot = keySize + Ints.BYTES * 2

            // Now just write the leaves.
            val itemCount = items.size
            val levelOffset = output.tell()
            for (i in 0 until itemCount step blockSize) {
                val leafCount = Math.min(itemCount - i, blockSize)
                with(output) {
                    writeBoolean(true)  // isLeaf.
                    writeByte(0)        // reserved.
                    writeShort(leafCount)
                    for (j in 0 until leafCount) {
                        items[i + j].write(output, keySize)
                    }

                    skipBytes(bytesInLeafSlot * (blockSize - leafCount))
                }
            }

            LOG.trace("Wrote ${items.size} leaves at leaf level " +
                      "(offset: $levelOffset)")
        }

        /** Writes out intermediate levels in a B+ tree. */
        private fun writeLevels(output: OrderedDataOutput, items: List<BPlusLeaf>,
                                blockSize: Int, keySize: Int) {
            val bytesInNodeHeader = 1 + 1 + Shorts.BYTES
            val bytesInIndexSlot = keySize + Longs.BYTES
            val bytesInIndexBlock = (bytesInNodeHeader + blockSize * bytesInIndexSlot).toLong()
            val bytesInLeafSlot = keySize + Ints.BYTES * 2
            val bytesInLeafBlock = (bytesInNodeHeader + blockSize * bytesInLeafSlot).toLong()

            // Write B+ tree levels top to bottom.
            val itemCount = items.size
            for (d in countLevels(blockSize, itemCount) - 1 downTo 1) {
                val levelOffset = output.tell()
                val itemsPerSlot = IntMath.pow(blockSize, d)
                val itemsPerNode = itemsPerSlot * blockSize
                val nodeCount = itemCount divCeiling itemsPerNode

                val bytesInCurrentLevel = nodeCount * bytesInIndexBlock
                val bytesInNextLevelBlock =
                        if (d == 1) bytesInLeafBlock else bytesInIndexBlock
                var childOffset = levelOffset + bytesInCurrentLevel
                for (i in 0 until itemCount step itemsPerNode) {
                    val childCount = Math.min((itemCount - i) divCeiling itemsPerSlot, blockSize)
                    with(output) {
                        writeBoolean(false)  // isLeaf.
                        writeByte(0)         // reserved.
                        writeShort(childCount)
                        for (j in 0 until Math.min(itemsPerNode, itemCount - i) step itemsPerSlot) {
                            BPlusNode(items[i + j].key, childOffset)
                                    .write(output, keySize)
                            childOffset += bytesInNextLevelBlock
                        }

                        skipBytes(bytesInIndexSlot * (blockSize - childCount))
                    }
                }

                LOG.trace("Wrote $nodeCount nodes at level $d (offset: $levelOffset)")
            }
        }

        /**
         * Counts the number of levels in a B+ tree for a given number
         * of items with fixed block size.
         *
         * Given block size (4 in the example) a B+ tree is laid out as:
         *
         *   [01|05]                          index level 1
         *   [01|02|03|04]   [05|06|07|08]    leaf  level 0
         *     ^               ^
         *    these are called blocks
         *
         * Conceptually, each B+ tree node consists of a number of
         * slots each holding `blockSize^level` items. So the
         * total number of items in a node can be calculated as
         * `blockSize^level * blockSize`.
         *
         * @param blockSize number of slots in a B+ tree node.
         * @param itemCount total number of leaves in a B+ tree
         * @return required number of levels.
         */
        internal fun countLevels(blockSize: Int, itemCount: Int): Int {
            return itemCount logCeiling blockSize
        }
    }
}

/**
 * A leaf in a B+ tree.
 */
data class BPlusLeaf(
        /** Chromosome name, e.g. "chr19" or "chrY". */
        val key: String,
        /** Unique chromosome identifier.  */
        val id: Int,
        /** Chromosome length in base pairs.  */
        val size: Int) {
    init {
        require(id >= 0) { "id must be >=0" }
        require(size >= 0) { "size must be >=0" }
    }

    internal fun write(output: OrderedDataOutput, keySize: Int) = with(output) {
        writeString(key, keySize)
        writeInt(id)
        writeInt(size)
    }

    override fun toString() = "$key => ($id; $size)"

    companion object {
        internal fun read(input: RomBuffer, keySize: Int) = with(input) {
            val keyBuf = readBytes(keySize)
            val chromId = readInt()
            val chromSize = readInt()

            BPlusLeaf(keyBuf.trimToString(keySize), chromId, chromSize)
            //TODO: benchmark
//            BPlusLeaf(String(keyBuf).trimEnd { it == '\u0000' }, chromId, chromSize)
        }
    }
}

/**
 * An item in a B+ tree.
 */
private data class BPlusNode(
        /** Chromosome name, e.g. "chr19" or "chrY". */
        val key: String,
        /** Offset to child node. */
        val childOffset: Long) {

    internal fun write(output: OrderedDataOutput, keySize: Int) = with(output) {
        writeString(key, keySize)
        writeLong(childOffset)
    }

    companion object {
        internal fun read(input: RomBuffer, keySize: Int) = with(input) {
            val keyBuf = readBytes(keySize)
            val childOffset = readLong()
            BPlusNode(keyBuf.trimToString(keySize), childOffset)
            //TODO: benchmark
//            BPlusNode(String(keyBuf).trimEnd { it == '\u0000' }, childOffset)
        }
    }
}

private val NULL_BYTE = 0.toByte()
fun ByteArray.trimToString(keySize: Int): String {
    var nameLen = keySize
    for (i in 0 until keySize) {
        if (this[i] == NULL_BYTE) {
            nameLen = i
            break
        }
    }
    return String(this, 0, nameLen)
}
