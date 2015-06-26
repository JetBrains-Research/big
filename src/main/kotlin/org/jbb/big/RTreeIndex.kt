package org.jbb.big

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.ListMultimap
import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.ArrayList

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
                val startChromIx = input.readInt()
                val startOffset = input.readInt()
                val endChromIx = input.readInt()
                val endOffset = input.readInt()
                val dataOffset = input.readLong()
                val dataSize = input.readLong()
                val interval = Interval.of(startChromIx, startOffset, endChromIx, endOffset)

                if (interval overlaps query) {
                    val backup = input.tell()
                    consumer(RTreeIndexLeaf(interval, dataOffset, dataSize))
                    input.seek(backup)
                }
            }
        } else {
            val children = ArrayList<RTreeIndexNode>(childCount.toInt())
            for (i in 0 until childCount) {
                val startChromIx = input.readInt()
                val startBase = input.readInt()
                val endChromIx = input.readInt()
                val endBase = input.readInt()
                val dataOffset = input.readLong()
                val interval = Interval.of(startChromIx, startBase, endChromIx, endBase)

                // XXX only add overlapping children, because there's no point
                // in storing all of them.
                if (interval overlaps query) {
                    children.add(RTreeIndexNode(interval, dataOffset))
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
            val itemArray = arrayOfNulls<bbiBoundsArray>(blockCount)
            for (i in 0 until blockCount) {
                itemArray[i] = bbiBoundsArray()
            }

            val doCompress = false
            RTreeIndexDetails.writeBlocks(usageList, bedPath, itemsPerSlot, itemArray,
                                          blockCount, doCompress, output, resTryCount,
                                          resScales, resSizes, bedSummary.itemCount,
                                          fieldCount)

            /* Write out primary data index. */
            val dataEndOffset = output.tell()
            val levelCount = wrapObject()
            var tree = RTreeIndexDetails.rTreeFromChromRangeArray(
                    blockSize, itemArray, dataEndOffset, levelCount)

            val dummyTree = rTree()
            dummyTree.startBase = 0 // struct rTree dummyTree = {.startBase=0};

            if (tree == null) {
                tree = rTree(dummyTree)        // Work for empty files....
            }

            val header = Header(output.order(), blockSize, itemArray.size().toLong(),
                                tree.startChromIx, tree.startBase,
                                tree.endChromIx, tree.endBase, dataEndOffset,
                                itemsPerSlot, output.tell() + RTreeIndex.Header.BYTES)
            header.write(output)

            if (tree != dummyTree) {
                RTreeIndexDetails.writeTreeToOpenFile(tree, blockSize, levelCount.toInt(), output)
            }

            return dataEndOffset
        }
    }
}

/**
 * External node aka *leaf* of the chromosome R-tree.
 */
data class RTreeIndexLeaf(public val interval: Interval,
                          public val dataOffset: Long,
                          public val dataSize: Long) {
}

/**
 * Internal node of the chromosome R-tree.
 */
data class RTreeIndexNode(public val interval: Interval,
                          public val dataOffset: Long)

class RTreeIndexWrapper(private val levels: List<List<Interval>>,
                        private val children: ListMultimap<Interval, Interval>) {

    companion object {
        fun of(intervals: List<Interval>, blockSize: Int): RTreeIndexWrapper {
            val children = ArrayListMultimap.create<Interval, Interval>()
            val levels = ArrayList<List<Interval>>()
            var acc = intervals
            while (acc.size() > 1) {
                // The size estimate is of course wrong, but it's a good
                // upper bound.
                val level = ArrayList<Interval>(acc.size() / blockSize)
                for (i in 0 until acc.size() step blockSize) {
                    // |-------|   parent
                    //   /   |
                    //  |-| |-|    links
                    val links = acc.subList(i, Math.min(acc.size(), i + blockSize))
                    if (links.size() == 1) {
                        level.addAll(links)
                    } else {
                        val parent = links.foldRight(Interval::union)
                        children[parent].addAll(links)
                        level.add(parent)
                    }
                }

                levels.add(level)
                acc = level
            }

            return RTreeIndexWrapper(levels, children)
        }
    }
}