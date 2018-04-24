package org.jetbrains.bio.big

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.jetbrains.bio.OrderedDataOutput
import org.jetbrains.bio.RomBuffer
import org.jetbrains.bio.divCeiling
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
 * super-intervals (aka slots) of size `itemsPerSlot`. The R+ tree
 * is then built over these slots.
 *
 * See tables 14-17 in the Supplementary Data for byte-to-byte details
 * on the R+ tree header and node formats.
 */
internal class RTreeIndex(val header: RTreeIndex.Header) {
    var rootNode: RTreeNode? = null

    /**
     * Recursively traverses an R+ tree calling `consumer` on each
     * block (aka leaf) overlapping a given `query`. Note that some
     * of the intervals contained in a block might *not* overlap the
     * `query`.
     */
    @Throws(IOException::class)
    fun findOverlappingBlocks(
            input: RomBuffer, query: Interval,
            cancelledChecker: (() -> Unit)?
    ): Sequence<RTreeIndexLeaf> {
        return if (header.itemCount == 0L) {
            emptySequence()
        } else {
            // if root node isn't prefetched - fetch it w/o expand
            // let's do not cache in order to make a 'memory leak' for
            // long running 
            if (rootNode == null) {
                findOverlappingBlocksRecursively(
                        input, query,
                        fetchRTreeNodeRecursively(input, false, false, header.rootOffset, cancelledChecker),
                        cancelledChecker)
            } else {
                findOverlappingBlocksRecursively(input, query, rootNode!!, cancelledChecker)
            }
        }
    }

    @Throws(IOException::class)
    fun prefetchBlocksIndex(
            input: RomBuffer,
            expandAll: Boolean,
            expandUpToChrs: Boolean,
            cancelledChecker: (() -> Unit)?
    ) {
        rootNode = if (header.itemCount == 0L) {
            null
        } else {
            fetchRTreeNodeRecursively(input, expandAll, expandUpToChrs, header.rootOffset, cancelledChecker)
        }
    }

    internal fun fetchRTreeNodeRecursively(
            input: RomBuffer,
            expandAll: Boolean,
            expandUpToChrs: Boolean,
            offset: Long,
            cancelledChecker: (() -> Unit)?
    ): RTreeNode {

        cancelledChecker?.invoke()

        assert(input.order == header.order)
        input.position = offset

        val isLeaf = input.readByte() > 0
        input.readByte()  // reserved.
        val childCount = input.readUnsignedShort()

        // XXX we have to eagerly read the blocks because 'input' is
        // shared between calls.

        return if (isLeaf) {
            val leaves = input.with(input.position, (childCount * RTreeIndexLeaf.BYTES).toLong()) {
                List(childCount) { RTreeIndexLeaf.read(this) }
            }
            RTReeNodeLeaf(leaves)
        } else {
            val nodes = input.with(input.position, (childCount * RTreeIndexNode.BYTES).toLong()) {
                List(childCount) {
                    RTreeIndexNode.read(this)
                }
            }
            val children = nodes.map { rTIndexNode ->
                val interval = rTIndexNode.interval
                if (expandAll || (expandUpToChrs && interval.left.chromIx != interval.right.chromIx)) {
                    RTreeNodeProxy(
                            fetchRTreeNodeRecursively(input, expandAll, expandUpToChrs, rTIndexNode.dataOffset, cancelledChecker),
                            interval
                    )
                } else {
                    RTreeNodeProxy(
                            RTreeNodeRef(rTIndexNode.dataOffset),
                            interval
                    )
                }
            }

            RTReeNodeIntermediate(children)
        }
    }

    private fun findOverlappingBlocksRecursively(
            input: RomBuffer,
            query: Interval,
            rootNode: RTreeNode,
            cancelledChecker: (() -> Unit)?
    ): Sequence<RTreeIndexLeaf> {

        cancelledChecker?.invoke()
        return when (rootNode) {
            is RTReeNodeLeaf -> rootNode.leaves.asSequence().filter { it.interval intersects query }
            else -> (rootNode as RTReeNodeIntermediate).children.asSequence()
                    .filter { it.interval intersects query }
                    .flatMap { proxy ->
                        val node = proxy.resolve(input, this, cancelledChecker)
                        findOverlappingBlocksRecursively(input, query, node, cancelledChecker)
                    }
        }
    }

    internal data class Header(val order: ByteOrder, val blockSize: Int,
                               val itemCount: Long,
                               val startChromIx: Int, val startBase: Int,
                               val endChromIx: Int, val endBase: Int,
                               val endDataOffset: Long, val itemsPerSlot: Int,
                               val rootOffset: Long) {

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
            internal const val BYTES = Ints.BYTES * 8 + Longs.BYTES * 2
            /** Magic number used for determining [ByteOrder]. */
            private const val MAGIC = 0x2468ACE0

            internal fun read(input: RomBuffer, offset: Long) = with(input) {
                val expectedOrder = order
                position = offset
                checkHeader(MAGIC)
                check(order == expectedOrder)

                val blockSize = readInt()
                val itemCount = readLong()
                val startChromIx = readInt()
                val startBase = readInt()
                val endChromIx = readInt()
                val endBase = readInt()
                val endDataOffset = readLong()
                val itemsPerSlot = readInt()
                readInt()  // reserved.
                val rootOffset = position

                Header(order, blockSize, itemCount, startChromIx, startBase,
                       endChromIx, endBase, endDataOffset, itemsPerSlot, rootOffset)
            }
        }
    }

    companion object {
        private val LOG = LogManager.getLogger(RTreeIndex::class.java)

        internal fun read(input: RomBuffer, offset: Long): RTreeIndex {
            return RTreeIndex(Header.read(input, offset))
        }

        internal fun write(output: OrderedDataOutput, leaves: List<RTreeIndexLeaf>,
                           blockSize: Int = 256, itemsPerSlot: Int = 512) {
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
            val rightmost = leaves.last().interval.right

            val header = Header(output.order, blockSize,
                                leaves.size.toLong(),
                                leftmost.chromIx, leftmost.offset,
                                rightmost.chromIx, rightmost.offset,
                                output.tell(), itemsPerSlot,
                                output.tell() + Header.BYTES)
            header.write(output)

            LOG.debug("Creating R+ tree for ${leaves.size} items " +
                      "($blockSize slots/node, $itemsPerSlot items/slot)")
            writeLevels(output, computeLevels(leaves, blockSize), blockSize)
            writeLeaves(output, leaves, blockSize)
            LOG.debug("Saved R+ tree using ${output.tell() - header.rootOffset} bytes")
        }

        /** Writes out final leaf level in an R+ tree. */
        private fun writeLeaves(output: OrderedDataOutput, leaves: List<RTreeIndexLeaf>,
                                blockSize: Int) {
            val levelOffset = output.tell()
            var i = 0
            while (i < leaves.size) {
                val leafCount = Math.min(blockSize, leaves.size - i)
                with(output) {
                    writeBoolean(true)  // isLeaf.
                    writeByte(0)        // reserved.
                    writeShort(leafCount)
                    for (j in 0 until leafCount) {
                        leaves[i + j].write(this)
                    }

                    // Write out zeroes for empty slots in node.
                    skipBytes(RTreeIndexLeaf.BYTES * (blockSize - leafCount))
                }

                i += blockSize
            }

            LOG.trace("Wrote ${leaves.size} slots at leaf level " +
                      "(offset: $levelOffset)")
        }

        /** Writes out intermediate levels in an R+ tree. */
        private fun writeLevels(output: OrderedDataOutput, levels: List<List<Interval>>,
                                blockSize: Int) {
            val bytesInNodeHeader = 1 + 1 + Shorts.BYTES
            val bytesInIndexBlock = bytesInNodeHeader + blockSize * RTreeIndexNode.BYTES
            val bytesInLeafBlock = bytesInNodeHeader + blockSize * RTreeIndexLeaf.BYTES
            for ((d, level) in levels.withIndex()) {
                val bytesInCurrentBlock = bytesInIndexBlock
                val bytesInNextLevelBlock =
                        if (d == levels.size - 1) bytesInLeafBlock else bytesInCurrentBlock

                val levelOffset = output.tell()
                val nodeCount = level.size divCeiling blockSize
                var childOffset = levelOffset + bytesInCurrentBlock * nodeCount
                var i = 0
                while (i < level.size) {
                    val childCount = Math.min(blockSize, level.size - i)
                    with(output) {
                        writeBoolean(false)  // isLeaf.
                        writeByte(0)         // reserved.
                        writeShort(childCount)
                        for (j in 0 until childCount) {
                            RTreeIndexNode(level[i + j], childOffset).write(this)
                            childOffset += bytesInNextLevelBlock
                        }

                        // Write out zeroes for empty slots in node.
                        skipBytes(RTreeIndexNode.BYTES * (blockSize - childCount))
                    }

                    i += blockSize
                }

                LOG.trace("Wrote ${level.size} slots at level $d (offset: $levelOffset)")
            }
        }

        private fun computeLevels(leaves: List<RTreeIndexLeaf>,
                                  blockSize: Int): List<List<Interval>> {
            var intervals = leaves.map { it.interval }
            if (LOG.isEnabledFor(Level.WARN)) {
                var containsIntersectedIntervalse = false
                for (i in 1 until intervals.size) {
                    if (intervals[i] intersects intervals[i - 1]) {
                        containsIntersectedIntervalse = true
                        LOG.debug("R+ tree leaves are overlapping: " +
                                 "${intervals[i]} ^ ${intervals[i - 1]}")
                    }
                }
                if (containsIntersectedIntervalse) {
                    LOG.warn("Some R+ tree leaves are overlapping. Queries might be not efficient.")
                }
            }

            val levels = arrayListOf(intervals)
            while (intervals.size > 1) {
                val level = ArrayList<Interval>(intervals.size divCeiling blockSize)
                var i = 0
                while (i < intervals.size) {
                    // |-------|   parent
                    //   /   |
                    //  |-| |-|    links
                    val links = intervals.subList(i, Math.min(intervals.size, i + blockSize))
                    level.add(links.reduce(Interval::union))
                    i += blockSize
                }

                levels.add(level)
                intervals = level
            }

            levels.reverse()
            LOG.debug("Computed ${levels.size} levels: ${levels.map { it.size }}")

            // Omit the root since it's trivial and leaves --- we'll deal
            // with them later.
            return levels.subList(1, Math.max(1, levels.size - 1))
        }
    }
}

internal interface RTreeNode
internal class RTReeNodeLeaf(val leaves: List<RTreeIndexLeaf>) : RTreeNode
internal open class RTReeNodeIntermediate(val children: List<RTreeNodeProxy>) : RTreeNode
internal data class RTreeNodeRef(val dataOffset: Long) : RTreeNode

internal class RTreeNodeProxy(internal val node: RTreeNode, val interval: Interval) {

    fun resolve(input: RomBuffer, rTreeIndex: RTreeIndex, cancelledChecker: (() -> Unit)?) = when (node) {
        is RTreeNodeRef -> rTreeIndex.fetchRTreeNodeRecursively(input, false, false, node.dataOffset, cancelledChecker)
        else -> node
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
        internal const val BYTES = Ints.BYTES * 4 + Longs.BYTES * 2

        internal fun read(input: RomBuffer) = with(input) {
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
 * Internal node of the chromosome R+ tree.
 */
internal data class RTreeIndexNode(
        val interval: Interval,
        val dataOffset: Long
) {
    internal fun write(output: OrderedDataOutput) = with(output) {
        interval.write(this)
        writeLong(dataOffset)
    }

    companion object {
        internal const val BYTES = Ints.BYTES * 4 + Longs.BYTES

        internal fun read(input: RomBuffer) = with(input) {
            val startChromIx = readInt()
            val startOffset = readInt()
            val endChromIx = readInt()
            val endOffset = readInt()
            val interval = Interval(startChromIx, startOffset, endChromIx, endOffset)
            RTreeIndexNode(interval, dataOffset = readLong())
        }
    }
}
