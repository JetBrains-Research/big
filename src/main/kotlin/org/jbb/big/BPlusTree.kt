package org.jbb.big

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import java.io.IOException
import java.nio.ByteOrder
import java.util.Optional
import java.util.function.Consumer
import kotlin.platform.platformStatic

/**
 * A B+ tree mapping chromosome names to (id, size) pairs.
 *
 * Here `id` is a unique positive integer and size is
 * chromosome length in base pairs. Contrary to the original
 * definition the leaves in this B+ tree aren't linked.
 *
 * See tables 8-11 in Supplementary Data for byte-to-byte details
 * on the B+ header and node formats.
 *
 * @author Sergey Zherevchuk
 * @author Sergei Lebedev
 * @since 13/03/15
 */
public class BPlusTree(val header: BPlusTree.Header) {
    /**
     * Recursively goes across tree, calling callback on the leaves.
     */
    throws(IOException::class)
    public fun traverse(input: SeekableDataInput, consumer: Consumer<BPlusItem>) {
        val originalOrder = input.order()
        input.order(header.byteOrder)
        traverseRecursively(input, header.rootOffset, consumer)
        input.order(originalOrder)
    }

    throws(IOException::class)
    private fun traverseRecursively(input: SeekableDataInput, blockStart: Long,
                                    consumer: Consumer<BPlusItem>) {
        // Invariant: a stream is in Header.byteOrder.
        input.seek(blockStart)

        val isLeaf = input.readBoolean()
        input.readBoolean()  // reserved
        val childCount = input.readShort().toInt()

        val keyBuf = ByteArray(header.keySize)
        if (isLeaf) {
            for (i in 0..childCount - 1) {
                input.readFully(keyBuf)
                val chromId = input.readInt()
                val chromSize = input.readInt()
                consumer.accept(BPlusItem(String(keyBuf).trimZeros(), chromId, chromSize))
            }
        } else {
            val fileOffsets = LongArray(childCount)
            for (i in 0..childCount - 1) {
                input.readFully(keyBuf)  // XXX why can we overwrite it?
                fileOffsets[i] = input.readLong()
            }

            for (i in 0..childCount - 1) {
                traverseRecursively(input, fileOffsets[i], consumer)
            }
        }
    }

    /**
     * Recursively traverses a B+ tree looking for a leaf corresponding
     * to `query`.
     */
    throws(IOException::class)
    public fun find(input: SeekableDataInput, query: String): Optional<BPlusItem> {
        if (query.length() > header.keySize) {
            return Optional.empty()
        }

        val originalOrder = input.order()
        input.order(header.byteOrder)

        // Trim query to 'keySize' because the spec. guarantees us
        // that all B+ tree nodes have a fixed-size key.
        val trimmedQuery = query.substring(0, Math.min(query.length(), header.keySize))
        val res = findRecursively(input, header.rootOffset, trimmedQuery)
        input.order(originalOrder)
        return Optional.ofNullable(res)
    }

    throws(IOException::class)
    private fun findRecursively(input: SeekableDataInput, blockStart: Long,
                                query: String): BPlusItem? {
        // Invariant: a stream is in Header.byteOrder.
        input.seek(blockStart)

        val isLeaf = input.readBoolean()
        input.readBoolean() // reserved
        val childCount = input.readShort()

        val keyBuf = ByteArray(header.keySize)
        if (isLeaf) {
            for (i in 0..childCount - 1) {
                input.readFully(keyBuf)
                val chromId = input.readInt()
                val chromSize = input.readInt()

                val key = String(keyBuf).trimZeros()
                if (query == key) {
                    return BPlusItem(key, chromId, chromSize)
                }
            }

            return null
        } else {
            input.readFully(keyBuf)
            var fileOffset = input.readLong()
            // vvv we loop from 1 because we've read the first child above.
            for (i in 1..childCount - 1) {
                input.readFully(keyBuf)
                if (query < String(keyBuf).trimZeros()) {
                    break
                }

                fileOffset = input.readLong()
            }

            return findRecursively(input, fileOffset, query)
        }
    }

    class Header(val byteOrder: ByteOrder, val blockSize: Int, val keySize: Int,
                 val itemCount: Long, val rootOffset: Long) {
        val valSize: Int = Ints.BYTES * 2  // (ID, Size)

        throws(IOException::class)
        fun write(output: SeekableDataOutput) = with(output) {
            writeInt(MAGIC)
            writeInt(blockSize)
            writeInt(keySize)
            writeInt(valSize)
            writeLong(itemCount)
            writeLong(0L)  // reserved
        }

        companion object {
            /** Number of bytes used for this header. */
            val BYTES = Ints.BYTES * 4 + Longs.BYTES * 2
            /** Magic number used for determining [ByteOrder]. */
            val MAGIC = 0x78CA8C91

            throws(IOException::class)
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

                return Header(order(), blockSize, keySize, itemCount, rootOffset)
            }
        }
    }

    companion object {
        throws(IOException::class)
        public platformStatic fun read(s: SeekableDataInput, offset: Long): BPlusTree {
            val header = Header.read(s, offset)
            return BPlusTree(header)
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
         * slots each holding {@code blockSize^level} items. So the
         * total number of items in a node can be calculated as
         * {@code blockSize^level * blockSize}
         *
         * @param blockSize number of slots in a B+ tree node.
         * @param itemCount total number of leaves in a B+ tree
         * @return required number of levels.
         */
        fun countLevels(blockSize: Int, itemCount: Int): Int {
            var acc = itemCount
            var levels = 1
            while (acc > blockSize) {
                acc = acc divCeiling blockSize
                levels++
            }

            return levels
        }

        throws(IOException::class)
        fun write(output: SeekableDataOutput, blockSize: Int, unsortedItems: List<BPlusItem>) {
            val items = unsortedItems.sortBy { it.key }

            val itemCount = items.size()
            val keySize = items.map { it.key.length() }.max()!!

            val header = Header(output.order(), blockSize, keySize,
                                itemCount.toLong(),
                                output.tell() + Header.BYTES)
            header.write(output)
            var indexOffset = header.rootOffset

            // HEAVY COMPUTER SCIENCE CALCULATION!
            val bytesInNodeHeader = 1 + 1 + Shorts.BYTES
            val bytesInIndexSlot = keySize + Longs.BYTES
            val bytesInIndexBlock = (bytesInNodeHeader + blockSize * bytesInIndexSlot).toLong()
            val bytesInLeafSlot = keySize + header.valSize
            val bytesInLeafBlock = (bytesInNodeHeader + blockSize * bytesInLeafSlot).toLong()

            // Write B+ tree levels top to bottom.
            val levels = countLevels(blockSize, items.size())
            for (level in levels - 1 downTo 1) {
                val itemsPerSlot = blockSize pow level
                val itemsPerNode = itemsPerSlot * blockSize
                val nodeCount = itemCount divCeiling itemsPerNode

                val bytesInCurrentLevel = nodeCount * bytesInIndexBlock
                val bytesInNextLevelBlock =
                        if (level == 1) bytesInLeafBlock else bytesInIndexBlock
                indexOffset += bytesInCurrentLevel
                var nextChild = indexOffset
                for (i in 0 until itemCount step itemsPerNode) {
                    val childCount = Math.min((itemCount - i) divCeiling itemsPerSlot, blockSize)
                    with (output) {
                        writeByte(0)  // isLeaf.
                        writeByte(0)  // reserved.
                        writeShort(childCount)
                        for (j in 0 until Math.min(itemsPerNode, itemCount - i) step itemsPerSlot) {
                            writeBytes(items[i + j].key, keySize)
                            writeLong(nextChild)
                            nextChild += bytesInNextLevelBlock
                        }

                        writeByte(0, bytesInIndexSlot * (blockSize - childCount))
                    }
                }
            }

            // Now just write the leaves.
            for (i in 0 until itemCount step blockSize) {
                val childCount = Math.min(itemCount - i, blockSize)
                with(output) {
                    writeByte(1)  // isLeaf.
                    writeByte(0)  // reserved.
                    writeShort(childCount)
                    for (j in 0 until childCount) {
                        val item = items[i + j]
                        writeBytes(item.key, keySize)
                        writeInt(item.id)
                        writeInt(item.size)
                    }

                    writeByte(0, bytesInLeafSlot * (blockSize - childCount))
                }
            }
        }
    }
}

/**
 * An item in a B+ tree.
 *
 * @author Sergey Zherevchuk
 * @since 10/03/15
 */
data class BPlusItem(
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

    override fun toString(): String {
        return "$key => ($id; $size)"
    }
}