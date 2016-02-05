package org.jetbrains.bio.big

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import org.apache.log4j.LogManager
import org.jetbrains.bio.*
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*

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
internal class RTreeIndex(val header: RTreeIndex.Header) {
    /**
     * Recursively traverses an R+ tree calling `consumer` on each
     * block (aka leaf) overlapping a given `query`. Note that some
     * of the intervals contained in a block might *not* overlap the
     * `query`.
     */
    @Throws(IOException::class)
    fun findOverlappingBlocks(input: SeekableDataInput,
                              query: ChromosomeInterval): Sequence<RTreeIndexLeaf> {
        return if (header.itemCount == 0L) {
            emptySequence()
        } else {
            findOverlappingBlocksRecursively(input, query, header.rootOffset)
        }
    }

    internal fun findOverlappingBlocksRecursively(input: SeekableDataInput,
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
            val acc = ArrayList<RTreeIndexLeaf>(childCount)
            input.with(input.tell(), (childCount * RTreeIndexLeaf.BYTES).toLong()) {
                for (i in 0..childCount - 1) {
                    val leaf = RTreeIndexLeaf.read(this)
                    if (leaf.interval intersects query) {
                        acc.add(leaf)
                    }
                }
            }

            acc.asSequence()
        } else {
            val acc = ArrayList<RTreeIndexNode>(childCount)
            input.with(input.tell(), (childCount * RTreeIndexNode.BYTES).toLong()) {
                for (i in 0..childCount - 1) {
                    val node = RTreeIndexNode.read(this)
                    if (node.interval intersects query) {
                        acc.add(node)
                    }
                }
            }

            acc.asSequence().flatMap { node ->
                findOverlappingBlocksRecursively(input, query, node.dataOffset)
            }
        }
    }

    internal class Header(val order: ByteOrder, val blockSize: Int, val itemCount: Long,
                          val startChromIx: Int, val startBase: Int,
                          val endChromIx: Int, val endBase: Int,
                          val endDataOffset: Long, val itemsPerSlot: Int, val rootOffset: Long) {

        internal fun write(output: OrderedDataOutput) = with(output) {
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
            internal val BYTES = Ints.BYTES * 8 + Longs.BYTES * 2
            /** Magic number used for determining [ByteOrder]. */
            private val MAGIC = 0x2468ACE0

            internal fun read(input: ByteBuffer, offset: Long) = with(input) {
                position(Ints.checkedCast(offset))
                guess(MAGIC)

                val blockSize = getInt()
                val itemCount = getLong()
                val startChromIx = getInt()
                val startBase = getInt()
                val endChromIx = getInt()
                val endBase = getInt()
                val endDataOffset = getLong()
                val itemsPerSlot = getInt()
                getInt()  // reserved.
                val rootOffset = position().toLong()

                Header(order(), blockSize, itemCount, startChromIx, startBase,
                       endChromIx, endBase, endDataOffset, itemsPerSlot, rootOffset)
            }
        }
    }

    companion object {
        private val LOG = LogManager.getLogger(RTreeIndex::class.java)

        internal fun read(input: ByteBuffer, offset: Long): RTreeIndex {
            return RTreeIndex(Header.read(input, offset))
        }

        internal fun write(output: CountingDataOutput, leaves: List<RTreeIndexLeaf>,
                           blockSize: Int = 256, itemsPerSlot: Int = 512): Unit {
            require(blockSize > 1) { "blockSize must be >1" }

            if (leaves.isEmpty()) {
                Header(output.order, blockSize,
                        leaves.size.toLong(),
                        0, 0, 0, 0,
                        output.tell(), itemsPerSlot,
                        output.tell() + Header.BYTES).write(output)

                return
            }

            val leftmost = leaves.first().interval.left
            var rightmost = leaves.last().interval.right

            val header = Header(output.order, blockSize,
                                leaves.size.toLong(),
                                leftmost.chromIx, leftmost.offset,
                                rightmost.chromIx, rightmost.offset,
                                output.tell(), itemsPerSlot,
                                output.tell() + Header.BYTES)
            header.write(output)

            LOG.debug("Creating R+ tree for ${leaves.size} items " +
                      "($blockSize slots/node, $itemsPerSlot items/slot)")

            // HEAVY COMPUTER SCIENCE CALCULATION!
            val bytesInNodeHeader = 1 + 1 + Shorts.BYTES
            val bytesInIndexBlock = bytesInNodeHeader + blockSize * RTreeIndexNode.BYTES
            val bytesInLeafBlock = bytesInNodeHeader + blockSize * RTreeIndexLeaf.BYTES

            val levels = compute(leaves, blockSize)
            for ((d, level) in levels.withIndex()) {
                val bytesInCurrentBlock = bytesInIndexBlock
                val bytesInNextLevelBlock =
                        if (d == levels.size - 1) bytesInLeafBlock else bytesInCurrentBlock

                val levelOffset = output.tell()
                val nodeCount = level.size divCeiling blockSize
                var childOffset = levelOffset + bytesInCurrentBlock * nodeCount
                for (i in 0..level.size - 1 step blockSize) {
                    val childCount = Math.min(blockSize, level.size - i)
                    with(output) {
                        writeBoolean(false)  // isLeaf.
                        writeByte(0)         // reserved.
                        writeShort(childCount)
                        for (j in 0..childCount - 1) {
                            RTreeIndexNode(level[i + j], childOffset).write(this)
                            childOffset += bytesInNextLevelBlock
                        }

                        // Write out zeroes for empty slots in node.
                        skipBytes(RTreeIndexNode.BYTES * (blockSize - childCount))
                    }
                }

                LOG.trace("Wrote ${level.size} slots at level $d (offset: $levelOffset)")
            }

            val levelOffset = output.tell()
            for (i in 0..leaves.size - 1 step blockSize) {
                val leafCount = Math.min(blockSize, leaves.size - i)
                with(output) {
                    writeBoolean(true)  // isLeaf.
                    writeByte(0)        // reserved.
                    writeShort(leafCount)
                    for (j in 0..leafCount - 1) {
                        leaves[i + j].write(this)
                    }

                    // Write out zeroes for empty slots in node.
                    skipBytes(RTreeIndexLeaf.BYTES * (blockSize - leafCount))
                }
            }

            LOG.trace("Wrote ${leaves.size} slots at level ${levels.size} " +
                      "(offset: $levelOffset)")
            LOG.debug("Saved R+ tree using ${output.tell() - header.rootOffset} bytes")
        }

        private fun compute(leaves: List<RTreeIndexLeaf>,
                            blockSize: Int): List<List<Interval>> {
            var intervals: List<Interval> = leaves.map { it.interval }
            for (i in 1..intervals.size - 1) {
                if (intervals[i] intersects intervals[i - 1]) {
                    LOG.warn("R+ tree leaves are overlapping: " +
                             "${intervals[i]} ^ ${intervals[i - 1]}")
                }
            }

            val levels = arrayListOf(intervals)
            while (intervals.size > 1) {
                val level = ArrayList<Interval>(intervals.size divCeiling blockSize)
                for (i in 0..intervals.size - 1 step blockSize) {
                    // |-------|   parent
                    //   /   |
                    //  |-| |-|    links
                    val links = intervals.subList(i, Math.min(intervals.size, i + blockSize))
                    level.add(links.reduce(Interval::union))
                }

                levels.add(level)
                intervals = level
            }

            Collections.reverse(levels)
            LOG.debug("Computed ${levels.size} levels: ${levels.map { it.size }}")

            // Omit the root since it's trivial and leaves --- we'll deal
            // with them later.
            return levels.subList(1, Math.max(1, levels.size - 1))
        }
    }
}

/**
 * External node aka *leaf* of the chromosome R+ tree.
 */
data class RTreeIndexLeaf(val interval: Interval,
                          val dataOffset: Long,
                          val dataSize: Long) {
    internal fun write(output: OrderedDataOutput) = with(output) {
        interval.write(this)
        writeLong(dataOffset)
        writeLong(dataSize)
    }

    companion object {
        internal val BYTES = Ints.BYTES * 4 + Longs.BYTES * 2

        internal fun read(input: ByteBuffer) = with(input) {
            val startChromIx = getInt()
            val startOffset = getInt()
            val endChromIx = getInt()
            val endOffset = getInt()
            val interval = Interval(startChromIx, startOffset, endChromIx, endOffset)
            RTreeIndexLeaf(interval, dataOffset = getLong(), dataSize = getLong())
        }
    }
}

/**
 * Internal node of the chromosome R+ tree.
 */
private data class RTreeIndexNode(val interval: Interval,
                                  val dataOffset: Long) {
    internal fun write(output: OrderedDataOutput) = with(output) {
        interval.write(this)
        writeLong(dataOffset)
    }

    companion object {
        internal val BYTES = Ints.BYTES * 4 + Longs.BYTES

        internal fun read(input: ByteBuffer) = with(input) {
            val startChromIx = getInt()
            val startOffset = getInt()
            val endChromIx = getInt()
            val endOffset = getInt()
            val interval = Interval(startChromIx, startOffset, endChromIx, endOffset)
            RTreeIndexNode(interval, dataOffset = getLong())
        }
    }
}