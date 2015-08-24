package org.jetbrains.bio.big

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import org.apache.log4j.LogManager
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
public class BPlusTree(val header: BPlusTree.Header) {
    /**
     * Recursively goes across tree, calling callback on the leaves.
     */
    @throws(IOException::class)
    public fun traverse(input: SeekableDataInput): Sequence<BPlusLeaf> {
        return traverseRecursively(input, header.rootOffset)
    }

    private fun traverseRecursively(input: SeekableDataInput,
                                    offset: Long): Sequence<BPlusLeaf> {
        assert(input.order == header.order)
        input.seek(offset)

        val isLeaf = input.readBoolean()
        input.readByte()  // reserved.
        val childCount = input.readUnsignedShort()

        return if (isLeaf) {
            (0..childCount - 1)
                    .mapUnboxed { BPlusLeaf.read(input, header.keySize) }
                    .toList().asSequence()
        } else {
            (0..childCount - 1)
                    .mapUnboxed { BPlusNode.read(input, header.keySize) }
                    .toList().asSequence()
                    .flatMap { traverseRecursively(input, it.childOffset) }
        }
    }

    /**
     * Recursively traverses a B+ tree looking for a leaf corresponding
     * to `query`.
     */
    @throws(IOException::class)
    public fun find(input: SeekableDataInput, query: String): BPlusLeaf? {
        if (query.length() > header.keySize) {
            return null
        }

        // Trim query to 'keySize' because the spec. guarantees us
        // that all B+ tree nodes have a fixed-size key.
        val trimmedQuery = query.substring(0, Math.min(query.length(), header.keySize))
        return findRecursively(input, header.rootOffset, trimmedQuery)
    }

    private fun findRecursively(input: SeekableDataInput, blockStart: Long,
                                query: String): BPlusLeaf? {
        assert(input.order == header.order)
        input.seek(blockStart)

        val isLeaf = input.readBoolean()
        input.readByte()  // reserved.
        val childCount = input.readUnsignedShort()

        if (isLeaf) {
            for (i in 0..childCount - 1) {
                val leaf = BPlusLeaf.read(input, header.keySize)
                if (leaf.key == query) {
                    return leaf
                }
            }

            return null
        } else {
            var node = BPlusNode.read(input, header.keySize)
            // vvv we loop from 1 because we've read the first child above.
            for (i in 1..childCount - 1) {
                val next = BPlusNode.read(input, header.keySize)
                if (query < next.key) {
                    break
                }

                node = next
            }

            return findRecursively(input, node.childOffset, query)
        }
    }

    class Header(val order: ByteOrder, val blockSize: Int, val keySize: Int,
                 val itemCount: Int, val rootOffset: Long) {
        val valSize: Int = Ints.BYTES * 2  // (ID, Size)

        fun write(output: OrderedDataOutput) = with(output) {
            writeInt(MAGIC)
            writeInt(blockSize)
            writeInt(keySize)
            writeInt(valSize)
            writeLong(itemCount.toLong())
            writeLong(0L)  // reserved
        }

        companion object {
            /** Number of bytes used for this header. */
            val BYTES = Ints.BYTES * 4 + Longs.BYTES * 2
            /** Magic number used for determining [ByteOrder]. */
            private val MAGIC = 0x78CA8C91

            fun read(input: SeekableDataInput, offset: Long): Header = with(input) {
                seek(offset)
                guess(MAGIC)
                val blockSize = readInt()
                val keySize = readInt()
                val valSize = readInt()
                check(valSize == Ints.BYTES * 2, "inconsistent value size: $valSize")

                val itemCount = readLong()
                readLong()  // reserved.
                val rootOffset = tell()

                return Header(order, blockSize, keySize,
                              Ints.checkedCast(itemCount), rootOffset)
            }
        }
    }

    companion object {
        private val LOG = LogManager.getLogger(javaClass)

        fun read(input: SeekableDataInput, offset: Long): BPlusTree {
            return BPlusTree(Header.read(input, offset))
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
        fun countLevels(blockSize: Int, itemCount: Int) = itemCount logCeiling blockSize

        fun write(output: CountingDataOutput, unsortedItems: List<BPlusLeaf>,
                  blockSize: Int = 256) {
            require(unsortedItems.isNotEmpty(), "no data")
            require(blockSize > 1, "blockSize must be >1")

            val items = unsortedItems.sortBy { it.key }
            val itemCount = items.size()
            val keySize = items.map { it.key.length() }.max()!!

            val header = Header(output.order, blockSize, keySize, itemCount,
                                output.tell() + Header.BYTES)
            header.write(output)

            LOG.debug("Creating a B+ tree for $itemCount items ($blockSize slots/node)")

            // HEAVY COMPUTER SCIENCE CALCULATION!
            val bytesInNodeHeader = 1 + 1 + Shorts.BYTES
            val bytesInIndexSlot = keySize + Longs.BYTES
            val bytesInIndexBlock = (bytesInNodeHeader + blockSize * bytesInIndexSlot).toLong()
            val bytesInLeafSlot = keySize + header.valSize
            val bytesInLeafBlock = (bytesInNodeHeader + blockSize * bytesInLeafSlot).toLong()

            // Write B+ tree levels top to bottom.
            val levels = countLevels(blockSize, items.size())
            for (d in levels - 1 downTo 1) {
                val levelOffset = output.tell()
                val itemsPerSlot = blockSize pow d
                val itemsPerNode = itemsPerSlot * blockSize
                val nodeCount = itemCount divCeiling itemsPerNode

                val bytesInCurrentLevel = nodeCount * bytesInIndexBlock
                val bytesInNextLevelBlock =
                        if (d == 1) bytesInLeafBlock else bytesInIndexBlock
                var childOffset = levelOffset + bytesInCurrentLevel
                for (i in 0..itemCount - 1 step itemsPerNode) {
                    val childCount = Math.min((itemCount - i) divCeiling itemsPerSlot, blockSize)
                    with(output) {
                        writeBoolean(false)  // isLeaf.
                        writeByte(0)         // reserved.
                        writeShort(childCount)
                        for (j in 0..Math.min(itemsPerNode, itemCount - i) - 1 step itemsPerSlot) {
                            BPlusNode(items[i + j].key, childOffset)
                                    .write(output, keySize)
                            childOffset += bytesInNextLevelBlock
                        }

                        skipBytes(bytesInIndexSlot * (blockSize - childCount))
                    }
                }

                LOG.trace("Wrote $nodeCount nodes at level $d (offset: $levelOffset)")
            }

            // Now just write the leaves.
            val levelOffset = output.tell()
            for (i in 0..itemCount - 1 step blockSize) {
                val leafCount = Math.min(itemCount - i, blockSize)
                with(output) {
                    writeBoolean(true)  // isLeaf.
                    writeByte(0)        // reserved.
                    writeShort(leafCount)
                    for (j in 0..leafCount - 1) {
                        items[i + j].write(output, keySize)
                    }

                    skipBytes(bytesInLeafSlot * (blockSize - leafCount))
                }
            }

            LOG.trace("Wrote ${items.size()} leaves at level $levels " +
                      "(offset: $levelOffset)")
            LOG.debug("Saved B+ tree using ${output.tell() - header.rootOffset} bytes")
        }
    }
}

/**
 * A leaf in a B+ tree.
 */
public data class BPlusLeaf(
        /** Chromosome name, e.g. "chr19" or "chrY". */
        public val key: String,
        /** Unique chromosome identifier.  */
        public val id: Int,
        /** Chromosome length in base pairs.  */
        public val size: Int) {
    init {
        require(id >= 0, "id must be >=0")
        require(size >= 0, "size must be >=0")
    }

    fun write(output: OrderedDataOutput, keySize: Int) = with(output) {
        writeBytes(key, keySize)
        writeInt(id)
        writeInt(size)
    }

    override fun toString(): String = "$key => ($id; $size)"

    companion object {
        fun read(input: OrderedDataInput, keySize: Int) = with(input) {
            val keyBuf = ByteArray(keySize)
            readFully(keyBuf)
            val chromId = readInt()
            val chromSize = readInt()
            BPlusLeaf(String(keyBuf).trimZeros(), chromId, chromSize)
        }
    }
}

/**
 * An item in a B+ tree.
 */
private class BPlusNode(
        /** Chromosome name, e.g. "chr19" or "chrY". */
        public val key: String,
        /** Offset to child node. */
        public val childOffset: Long) {

    fun write(output: OrderedDataOutput, keySize: Int) = with(output) {
        writeBytes(key, keySize)
        writeLong(childOffset)
    }

    companion object {
        fun read(input: OrderedDataInput, keySize: Int) = with(input) {
            val keyBuf = ByteArray(keySize)
            readFully(keyBuf)
            val childOffset = readLong()
            BPlusNode(String(keyBuf).trimZeros(), childOffset)
        }
    }
}