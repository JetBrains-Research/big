package org.jbb.big

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ListMultimap
import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.ArrayList
import java.util.Collections

/**
 * A 1-D R+ tree for storing genomic intervals.
 *
 * TODO: explain that we don't index all intervals when building a tree.
 * TODO: explain that the 1-D R+ tree is simply a range tree build with
 *       using interval union.
 *
 * See tables 14-17 in the Supplementary Data for byte-to-byte details
 * on the R+ tree header and node formats.
 *
 * @author Sergey Zherevchuk
 * @author Sergei Lebedev
 * @since 13/03/15
 */
class RTreeIndex(val header: RTreeIndex.Header) {
    /**
     * Recursively traverses an R+ tree calling `consumer` on each
     * block (aka leaf) overlapping a given `query`. Note that some
     * of the intervals contained in a block might *not* overlap the
     * `query`.
     */
    throws(IOException::class)
    fun findOverlappingBlocks(s: SeekableDataInput, query: Interval,
                                        consumer: (RTreeIndexLeaf) -> Unit) {
        val originalOrder = s.order()
        s.order(header.byteOrder)
        try {
            findOverlappingBlocksRecursively(s, query, header.rootOffset, consumer)
        } finally {
            s.order(originalOrder)
        }
    }

    throws(IOException::class)
    private fun findOverlappingBlocksRecursively(input: SeekableDataInput,
                                                 query: Interval, offset: Long,
                                                 consumer: (RTreeIndexLeaf) -> Unit) {
        assert(input.order() == header.byteOrder)
        input.seek(offset)

        val isLeaf = input.readBoolean()
        input.readBoolean()  // reserved.
        val childCount = input.readShort().toInt()

        if (isLeaf) {
            for (i in 0 until childCount) {
                val leaf = RTreeIndexLeaf.read(input)
                if (leaf.interval overlaps query) {
                    val backup = input.tell()
                    consumer(leaf)
                    input.seek(backup)
                }
            }
        } else {
            val children = ArrayList<RTreeIndexNode>(childCount.toInt())
            for (i in 0 until childCount) {
                // XXX only add overlapping children, because there's no point
                // in storing all of them.
                val node = RTreeIndexNode.read(input)
                if (node.interval overlaps query) {
                    children.add(node)
                }
            }

            for (node in children) {
                findOverlappingBlocksRecursively(input, query, node.dataOffset, consumer)
            }
        }
    }

    class Header(val byteOrder: ByteOrder, val blockSize: Int, val itemCount: Long,
                 val startChromIx: Int, val startBase: Int,
                 val endChromIx: Int, val endBase: Int,
                 val endDataOffset: Long, val itemsPerSlot: Int, val rootOffset: Long) {
        throws(IOException::class)
        fun write(output: SeekableDataOutput) = with(output) {
            writeInt(MAGIC)
            writeInt(blockSize)
            writeLong(itemCount)
            writeInt(startChromIx)
            writeInt(startBase)
            writeInt(endChromIx)
            writeInt(endBase)
            writeLong(endDataOffset)
            writeInt(itemsPerSlot)
            writeInt(0)  // reserved.
        }

        companion object {
            /** Number of bytes used for this header. */
            val BYTES = Ints.BYTES * 8 + Longs.BYTES * 2
            /** Magic number used for determining [ByteOrder]. */
            private val MAGIC = 0x2468ace0

            throws(IOException::class)
            fun read(input: SeekableDataInput, offset: Long): Header = with(input) {
                seek(offset)
                guess(MAGIC)

                val blockSize = readInt()
                val itemCount = readLong()
                val startChromIx = readInt()
                val startBase = readInt()
                val endChromIx = readInt()
                val endBase = readInt()
                val endDataOffset = readLong()
                val itemsPerSlot = readInt()
                readInt()  // reserved.
                val rootOffset = tell()

                return Header(order(), blockSize, itemCount, startChromIx, startBase,
                              endChromIx, endBase, endDataOffset, itemsPerSlot, rootOffset)
            }
        }
    }

    companion object {
        throws(IOException::class)
        public fun read(input: SeekableDataInput, offset: Long): RTreeIndex {
            return RTreeIndex(Header.read(input, offset))
        }

        public fun countBlocks(usageList: List<bbiChromUsage>, itemsPerSlot: Int): Int {
            var count = 0
            for (usage in usageList) {
                count += usage.itemCount divCeiling itemsPerSlot
            }
            return count
        }

        throws(IOException::class)
        public fun write(output: SeekableDataOutput, chromSizesPath: Path,
                         bedPath: Path, fieldCount: Short,
                         blockSize: Int = 1024, itemsPerSlot: Int = 64): Long {
            val bedSummary = BedSummary.of(bedPath, chromSizesPath)
            val usageList = bedSummary.toList()

            val resScales = IntArray(RTreeIndexDetails.bbiMaxZoomLevels)
            val resSizes = IntArray(RTreeIndexDetails.bbiMaxZoomLevels)
            val resTryCount = RTreeIndexDetails.bbiCalcResScalesAndSizes(
                    bedSummary.baseCount / bedSummary.itemCount, resScales, resSizes)

            val blockCount = countBlocks(usageList, itemsPerSlot)
            val itemArray = (0 until blockCount).map { bbiBoundsArray() }.toTypedArray()

            val doCompress = false
            RTreeIndexDetails.writeBlocks(usageList, bedPath, itemsPerSlot, itemArray,
                                          blockCount, doCompress, output, resTryCount,
                                          resScales, resSizes, bedSummary.itemCount,
                                          fieldCount)

            /* Write out primary data index. */
            val dataEndOffset = output.tell()
            val offsets = itemArray.map { it.offset }.toLongArray()
            val leaves = itemArray.map { it.range }.mapIndexed { i, range ->
                val interval = ChromosomeInterval(range.chromIx, range.start, range.end)
                val endOffset = if (i < offsets.size() - 1) offsets[i + 1] else dataEndOffset
                val size = endOffset - offsets[i]
                RTreeIndexLeaf(interval, offsets[i], size)
            }.toList()

            val wrapper = RTreeIndexWrapper.of(leaves, blockSize)
            val header = Header(output.order(), blockSize, blockCount.toLong(),
                                wrapper.left.chromIx, wrapper.left.offset,
                                wrapper.right.chromIx, wrapper.right.offset,
                                dataEndOffset, itemsPerSlot, output.tell() + Header.BYTES)
            header.write(output)
            wrapper.write(output, blockSize)

            return dataEndOffset
        }
    }
}

class RTreeIndexWrapper(private val levels: List<List<Interval>>,
                        private val leaves: List<RTreeIndexLeaf>) {
    private val DUMMY: Offset = Offset(0, 0)

    public val left: Offset
            = if (leaves.isEmpty()) DUMMY else levels.last().first().left
    public val right: Offset
            = if (leaves.isEmpty()) DUMMY else levels.last().last().right

    throws(IOException::class)
    fun write(output: SeekableDataOutput, blockSize: Int) {
        // HEAVY COMPUTER SCIENCE CALCULATION!
        val bytesInNodeHeader = 1 + 1 + Shorts.BYTES
        val bytesInIndexSlot = Ints.BYTES * 4 + Longs.BYTES
        val bytesInIndexBlock = bytesInNodeHeader + blockSize * bytesInIndexSlot
        val bytesInLeafSlot = Ints.BYTES * 4 + Longs.BYTES * 2
        val bytesInLeafBlock = bytesInNodeHeader + blockSize * bytesInLeafSlot

        // Omit root because it's trivial and leaves --- we'll deal
        // with them later.
        levels.subList(1, levels.size() - 1).forEachIndexed { i, level ->
            val bytesInCurrentBlock = bytesInIndexBlock
            val bytesInNextLevelBlock =
                    if (i == levels.size() - 3) bytesInLeafBlock else bytesInCurrentBlock
            var nextChild = output.tell() + bytesInCurrentBlock
            val nodeCount = level.size()
            with(output) {
                writeByte(0)  // isLeaf.
                writeByte(0)  // reserved.
                writeShort(nodeCount)
                for (interval in level) {
                    RTreeIndexNode(interval, nextChild).write(output)
                    nextChild += bytesInNextLevelBlock
                }

                // Write out zeroes for empty slots in node.
                writeByte(0, bytesInIndexSlot * (blockSize - nodeCount))
            }
        }

        with(output) {
            for (i in 0 until leaves.size() step blockSize) {
                val leafCount = Math.min(blockSize, leaves.size() - i)
                writeByte(1)  // isLeaf.
                writeByte(0)  // reserved.
                writeShort(leafCount)
                for (j in 0 until leafCount) {
                    leaves[i + j].write(output)
                }

                // Write out zeroes for empty slots in node.
                writeByte(0, bytesInLeafSlot * (blockSize - leafCount))
            }
        }
    }

    companion object {
        fun of(leaves: List<RTreeIndexLeaf>, blockSize: Int): RTreeIndexWrapper {
            var intervals: List<Interval> = leaves.map { it.interval }
            val levels = arrayListOf(intervals)
            while (intervals.size() > 1) {
                val level = ArrayList<Interval>(intervals.size() / blockSize)
                for (i in 0 until intervals.size() step blockSize) {
                    // |-------|   parent
                    //   /   |
                    //  |-| |-|    links
                    val links = intervals.subList(i, Math.min(intervals.size(), i + blockSize))
                    if (links.size() == 1) {
                        level.addAll(links)
                    } else {
                        level.add(links.reduce(Interval::union))
                    }
                }

                levels.add(level)
                intervals = level
            }

            Collections.reverse(levels)
            return RTreeIndexWrapper(levels, leaves)
        }
    }
}

/**
 * External node aka *leaf* of the chromosome R-tree.
 */
data class RTreeIndexLeaf(public val interval: ChromosomeInterval,
                          public val dataOffset: Long,
                          public val dataSize: Long) {
    fun write(output: SeekableDataOutput) = with(output) {
        writeInt(interval.chromIx)
        writeInt(interval.startOffset)
        writeInt(interval.chromIx)
        writeInt(interval.endOffset)
        writeLong(dataOffset)
        writeLong(dataSize)
    }

    companion object {
        fun read(input: SeekableDataInput): RTreeIndexLeaf = with (input) {
            val startChromIx = readInt()
            val startOffset = readInt()
            val endChromIx = readInt()
            val endOffset = readInt()
            assert(startChromIx == endChromIx)
            val interval = Interval.of(startChromIx, startOffset, endOffset)
            return RTreeIndexLeaf(interval, dataOffset = readLong(),
                                  dataSize = readLong())
        }
    }
}

/**
 * Internal node of the chromosome R-tree.
 */
data class RTreeIndexNode(public val interval: Interval,
                          public val dataOffset: Long) {
    fun write(output: SeekableDataOutput) = with(output) {
        writeInt(interval.left.chromIx)
        writeInt(interval.left.offset)
        writeInt(interval.right.chromIx)
        writeInt(interval.right.offset)
        writeLong(dataOffset)
    }

    companion object {
        fun read(input: SeekableDataInput): RTreeIndexNode = with (input) {
            val startChromIx = readInt()
            val startBase = readInt()
            val endChromIx = readInt()
            val endBase = readInt()
            val interval = Interval.of(startChromIx, startBase, endChromIx, endBase)
            return RTreeIndexNode(interval, dataOffset = readLong())
        }
    }
}