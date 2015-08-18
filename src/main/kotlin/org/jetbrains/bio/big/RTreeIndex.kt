package org.jetbrains.bio.big

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import org.apache.log4j.LogManager
import java.io.IOException
import java.nio.ByteOrder
import java.util.ArrayList
import java.util.Collections

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
 * super-intervals (aka slots) of size `itemsPerSlot`. The R+ tree
 * is then built over these slots.
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
    fun findOverlappingBlocks(input: SeekableDataInput,
                              query: ChromosomeInterval): Sequence<RTreeIndexLeaf> {
        return findOverlappingBlocksRecursively(input, query, header.rootOffset)
    }

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
                    .filter { it.interval intersects query }
                    .asSequence()
        } else {
            (0 until childCount)
                    .map { RTreeIndexNode.read(input) }
                    .filter { it.interval intersects query }
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

        fun write(output: OrderedDataOutput) = with(output) {
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
        private val LOG = LogManager.getLogger(javaClass)

        public fun read(input: SeekableDataInput, offset: Long): RTreeIndex {
            return RTreeIndex(Header.read(input, offset))
        }

        public fun write(output: CountingDataOutput,
                         leaves: List<RTreeIndexLeaf>,
                         blockSize: Int = 256,
                         itemsPerSlot: Int = 512): Unit {
            require(leaves.isNotEmpty(), "no data")
            require(blockSize > 1, "blockSize must be >1")

            LOG.debug("Creating R+ tree for ${leaves.size()} items " +
                      "($blockSize slots/node, $itemsPerSlot items/slot)")

            val leftmost = leaves.first().interval.left
            var rightmost = leaves.last().interval.right

            val header = Header(output.order, blockSize,
                                leaves.size().toLong(),
                                leftmost.chromIx, leftmost.offset,
                                rightmost.chromIx, rightmost.offset,
                                output.tell(), itemsPerSlot,
                                output.tell() + Header.BYTES)
            header.write(output)

            // HEAVY COMPUTER SCIENCE CALCULATION!
            val bytesInNodeHeader = 1 + 1 + Shorts.BYTES
            val bytesInIndexBlock = bytesInNodeHeader + blockSize * RTreeIndexNode.BYTES
            val bytesInLeafBlock = bytesInNodeHeader + blockSize * RTreeIndexLeaf.BYTES

            val levels = compute(leaves, blockSize)
            for ((d, level) in levels.withIndex()) {
                val bytesInCurrentBlock = bytesInIndexBlock
                val bytesInNextLevelBlock =
                        if (d == levels.size() - 1) bytesInLeafBlock else bytesInCurrentBlock

                val levelOffset = output.tell()
                var childOffset = levelOffset +
                                  bytesInCurrentBlock * (level.size() divCeiling blockSize)
                for (i in 0 until level.size() step blockSize) {
                    val childCount = Math.min(blockSize, level.size() - i)
                    with(output) {
                        writeBoolean(false)  // isLeaf.
                        writeByte(0)         // reserved.
                        writeUnsignedShort(childCount)
                        for (j in 0 until childCount) {
                            RTreeIndexNode(level[i + j], childOffset).write(this)
                            childOffset += bytesInNextLevelBlock
                        }

                        // Write out zeroes for empty slots in node.
                        skipBytes(0, RTreeIndexNode.BYTES * (blockSize - childCount))
                    }
                }

                LOG.debug("Wrote ${level.size()} items at level $d (offset: $levelOffset)")
            }

            val levelOffset = output.tell()
            for (i in 0 until leaves.size() step blockSize) {
                val leafCount = Math.min(blockSize, leaves.size() - i)
                with(output) {
                    writeBoolean(true)  // isLeaf.
                    writeByte(0)        // reserved.
                    writeUnsignedShort(leafCount)
                    for (j in 0 until leafCount) {
                        leaves[i + j].write(this)
                    }

                    // Write out zeroes for empty slots in node.
                    skipBytes(0, RTreeIndexLeaf.BYTES * (blockSize - leafCount))
                }
            }

            LOG.debug("Wrote ${leaves.size()} items at level ${levels.size()} " +
                      "(offset: $levelOffset)")
            LOG.debug("Saved R+ tree using ${output.tell() - header.rootOffset} bytes")
        }

        private fun compute(leaves: List<RTreeIndexLeaf>,
                            blockSize: Int): List<List<Interval>> {
            var intervals: List<Interval> = leaves.map { it.interval }
            val levels = arrayListOf(intervals)
            while (intervals.size() > 1) {
                val level = ArrayList<Interval>(intervals.size() divCeiling blockSize)
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

            Collections.reverse(levels)
            LOG.debug("Computed ${levels.size()} levels: ${levels.map { it.size() }}")

            // Omit the leaves --- we'll deal with them later.
            return levels.subList(1, Math.max(1, levels.size() - 1))
        }
    }
}

/**
 * External node aka *leaf* of the chromosome R-tree.
 */
data class RTreeIndexLeaf(public val interval: Interval,
                          public val dataOffset: Long,
                          public val dataSize: Long) {
    fun write(output: OrderedDataOutput) = with(output) {
        writeInt(interval.left.chromIx)
        writeInt(interval.left.offset)
        writeInt(interval.right.chromIx)
        writeInt(interval.right.offset)
        writeLong(dataOffset)
        writeLong(dataSize)
    }

    companion object {
        val BYTES = Ints.BYTES * 4 + Longs.BYTES * 2

        fun read(input: OrderedDataInput) = with(input) {
            val startChromIx = readInt()
            val startOffset = readInt()
            val endChromIx = readInt()
            val endOffset = readInt()
            val interval = Interval(startChromIx, startOffset, endChromIx, endOffset)
            RTreeIndexLeaf(interval, dataOffset = readLong(), dataSize = readLong())
        }
    }
}

/**
 * Internal node of the chromosome R-tree.
 */
data class RTreeIndexNode(public val interval: Interval,
                          public val dataOffset: Long) {
    fun write(output: OrderedDataOutput) = with(output) {
        writeInt(interval.left.chromIx)
        writeInt(interval.left.offset)
        writeInt(interval.right.chromIx)
        writeInt(interval.right.offset)
        writeLong(dataOffset)
    }

    companion object {
        val BYTES = Ints.BYTES * 4 + Longs.BYTES

        fun read(input: OrderedDataInput) = with(input) {
            val startChromIx = readInt()
            val startOffset = readInt()
            val endChromIx = readInt()
            val endOffset = readInt()
            val interval = Interval(startChromIx, startOffset, endChromIx, endOffset)
            RTreeIndexNode(interval, dataOffset = readLong())
        }
    }
}