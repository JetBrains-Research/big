package org.jetbrains.bio.big

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import java.io.IOException
import java.nio.ByteOrder

/**
 * A 1-D R+ tree for storing genomic intervals.
 *
 * The tree is built bottom-up by applying union operations to adjacent
 * intervals. The number of intervals combined between levels is
 * defined by the `blockSize` value. For instance for blockSize = 2:
 *
 *    |----------------|  index level 1
 *        /        \
 *    |-----| |--------|  index level 0
 *      / |     /    \
 *    |----|  |--| |---|  leaf  level
 *       |--|
 *
 * The Big format applies a simple trick to optimize tree height.
 * Prior to building the tree the intervals are combined into
 * super-intervals of size `itemsPerSlot`. The R+ tree is then built
 * over these super-intervals.
 *
 * See tables 14-17 in the Supplementary Data for byte-to-byte details
 * on the R+ tree header and node formats.
 */
class RTreeIndex(val header: RTreeIndex.Header) {
    /**
     * Recursively traverses an R+ tree calling `consumer` on each
     * block (aka leaf) overlapping a given `query`. Note that some
     * of the intervals contained in a block might *not* overlap the
     * `query`.
     */
    throws(IOException::class)
    fun findOverlappingBlocks(input: SeekableDataInput,
                              query: ChromosomeInterval): Sequence<RTreeIndexLeaf> {
        return findOverlappingBlocksRecursively(input, query, header.rootOffset)
    }

    throws(IOException::class)
    fun findOverlappingBlocksRecursively(input: SeekableDataInput,
                                         query: ChromosomeInterval,
                                         offset: Long): Sequence<RTreeIndexLeaf> {
        assert(input.order == header.order)
        input.seek(offset)

        val isLeaf = input.readBoolean()
        input.readByte()  // reserved.
        val childCount = input.readUnsignedShort()

        // XXX we have to eagerly read the blocks because 'input' is
        // shared between calls.
        return if (isLeaf) {
            (0 until childCount)
                    .map { RTreeIndexLeaf.read(input) }
                    .filter { it.interval overlaps query }
                    .asSequence()
        } else {
            (0 until childCount)
                    .map { RTreeIndexNode.read(input) }
                    .filter { it.interval overlaps query }
                    .asSequence()
                    .flatMap { node ->
                        findOverlappingBlocksRecursively(input, query,
                                                         node.dataOffset)
                    }
        }
    }

    class Header(val order: ByteOrder, val blockSize: Int, val itemCount: Long,
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
            private val MAGIC = 0x2468ACE0

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

                return Header(order, blockSize, itemCount, startChromIx, startBase,
                              endChromIx, endBase, endDataOffset, itemsPerSlot, rootOffset)
            }
        }
    }

    companion object {
        throws(IOException::class)
        public fun read(input: SeekableDataInput, offset: Long): RTreeIndex {
            return RTreeIndex(Header.read(input, offset))
        }

        throws(IOException::class)
        public fun write(output: SeekableDataOutput,
                         leaves: List<RTreeIndexLeaf>,
                         blockSize: Int = 256,
                         itemsPerSlot: Int = 512): Unit {
            val leftmost = leaves.first().interval
            var rightmost = leaves.last().interval

            val header = Header(output.order, blockSize,
                                leaves.size().toLong(),
                                leftmost.chromIx, leftmost.startOffset,
                                rightmost.chromIx, rightmost.endOffset,
                                output.tell(), itemsPerSlot,
                                output.tell() + Header.BYTES)
            header.write(output)

            val levels = compute(leaves, blockSize)

            // HEAVY COMPUTER SCIENCE CALCULATION!
            val bytesInNodeHeader = 1 + 1 + com.google.common.primitives.Shorts.BYTES
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
                    writeBoolean(false)  // isLeaf.
                    writeByte(0)         // reserved.
                    writeShort(nodeCount)
                    for (interval in level) {
                        RTreeIndexNode(interval, nextChild).write(output)
                        nextChild += bytesInNextLevelBlock
                    }

                    // Write out zeroes for empty slots in node.
                    skipBytes(0, bytesInIndexSlot * (blockSize - nodeCount))
                }
            }

            with(output) {
                for (i in 0 until leaves.size() step blockSize) {
                    val leafCount = Math.min(blockSize, leaves.size() - i)
                    writeBoolean(true)  // isLeaf.
                    writeByte(0)        // reserved.
                    writeShort(leafCount)
                    for (j in 0 until leafCount) {
                        leaves[i + j].write(output)
                    }

                    // Write out zeroes for empty slots in node.
                    skipBytes(0, bytesInLeafSlot * (blockSize - leafCount))
                }
            }
        }

        private fun compute(leaves: List<RTreeIndexLeaf>,
                            blockSize: Int): List<List<Interval>> {
            var intervals: List<Interval> = leaves.map { it.interval }
            val levels = arrayListOf(intervals)
            while (intervals.size() > 1) {
                val level = java.util.ArrayList<Interval>(intervals.size() / blockSize)
                for (i in 0 until intervals.size() step blockSize) {
                    // |-------|   parent
                    //   /   |
                    //  |-| |-|    links
                    val links = intervals.subList(i, Math.min(intervals.size(), i + blockSize))
                    level.add(links.reduce(Interval::union))
                }

                levels.add(level)
                intervals = level
            }

            java.util.Collections.reverse(levels)
            return levels
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
        fun read(input: SeekableDataInput) = with (input) {
            val startChromIx = readInt()
            val startOffset = readInt()
            val endChromIx = readInt()
            val endOffset = readInt()
            assert(startChromIx == endChromIx)
            val interval = Interval.of(startChromIx, startOffset, endOffset)
            RTreeIndexLeaf(interval, dataOffset = readLong(), dataSize = readLong())
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
        fun read(input: SeekableDataInput) = with (input) {
            val startChromIx = readInt()
            val startBase = readInt()
            val endChromIx = readInt()
            val endBase = readInt()
            val interval = Interval.of(startChromIx, startBase, endChromIx, endBase)
            RTreeIndexNode(interval, dataOffset = readLong())
        }
    }
}