package org.jbb.big

import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.ComparisonChain
import com.google.common.math.IntMath
import java.io.IOException
import java.math.RoundingMode
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.ArrayList
import java.util.Objects
import kotlin.platform.platformStatic

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
    fun findOverlappingBlocks(s: SeekableDataInput, query: RTreeInterval,
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
                                                 query: RTreeInterval, offset: Long,
                                                 consumer: (RTreeIndexLeaf) -> Unit) {
        // Invariant: a stream is in Header.byteOrder.
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
                val interval = RTreeInterval.of(startChromIx, startOffset, endChromIx, endOffset)

                if (interval.overlaps(query)) {
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
                val interval = RTreeInterval.of(startChromIx, startBase, endChromIx, endBase)

                // XXX only add overlapping children, because there's no point
                // in storing all of them.
                if (interval.overlaps(query)) {
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
                 val fileSize: Long, val itemsPerSlot: Int, val rootOffset: Long) {
        companion object {
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
                val fileSize = readLong()
                val itemsPerSlot = readInt()
                readInt()  // reserved.
                val rootOffset = tell()

                return Header(order(), blockSize, itemCount, startChromIx, startBase,
                              endChromIx, endBase, fileSize, itemsPerSlot, rootOffset)
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
                             bedPath: Path, blockSize: Int, itemsPerSlot: Int,
                             fieldCount: Short): Long {
                val bedSummary = BedSummary.of(bedPath, chromSizesPath)
                val usageList = bedSummary.toList()

                val resScales = IntArray(RTreeIndexDetails.bbiMaxZoomLevels)
                val resSizes = IntArray(RTreeIndexDetails.bbiMaxZoomLevels)
                val resTryCount = RTreeIndexDetails.bbiCalcResScalesAndSizes(
                        bedSummary.baseCount / bedSummary.itemCount, resScales, resSizes)

                val blockCount = countBlocks(usageList, itemsPerSlot)
                val boundsArray = arrayOfNulls<bbiBoundsArray>(blockCount)
                for (i in 0 until blockCount) {
                    boundsArray[i] = bbiBoundsArray()
                }

                val doCompress = false
                RTreeIndexDetails.writeBlocks(usageList, bedPath, itemsPerSlot, boundsArray,
                                              blockCount, doCompress, output, resTryCount,
                                              resScales, resSizes, bedSummary.itemCount,
                                              fieldCount)

                /* Write out primary data index. */
                val indexOffset = output.tell()
                RTreeIndexDetails.cirTreeFileBulkIndexToOpenFile(
                        boundsArray, blockCount.toLong(), blockSize, 1, indexOffset, output)

                return indexOffset
            }
        }
    }

    companion object {
        throws(IOException::class)
        public fun read(input: SeekableDataInput, offset: Long): RTreeIndex {
            return RTreeIndex(Header.read(input, offset))
        }
    }
}

/**
 * Chromosome R-tree external node format
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
data class RTreeIndexLeaf(public val interval: RTreeInterval,
                          public val dataOffset: Long,
                          public val dataSize: Long)

/**
 * Internal node of the chromosome R-tree.
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
data class RTreeIndexNode(public val interval: RTreeInterval, public val dataOffset: Long)

/**
 * A semi-closed interval.
 *
 * TODO: a more sound approach would be to separate the single-
 * multi- chromosome use-cases.
 *
 * @author Sergei Lebedev
 * @since 16/03/15
 */
data class RTreeInterval(
        /** Start offset (inclusive).  */
        public val left: RTreeOffset,
        /** End offset (exclusive).  */
        public val right: RTreeOffset) {

    public fun overlaps(other: RTreeInterval): Boolean {
        return !(other.right <= left || other.left >= right)
    }

    override fun toString(): String = "[$left; $right)"

    companion object {
        platformStatic fun of(chromIx: Int, startOffset: Int, endOffset: Int): RTreeInterval {
            return of(chromIx, startOffset, chromIx, endOffset)
        }

        platformStatic fun of(startChromIx: Int, startOffset: Int,
                              endChromIx: Int, endOffset: Int): RTreeInterval {
            return RTreeInterval(RTreeOffset(startChromIx, startOffset),
                                 RTreeOffset(endChromIx, endOffset))
        }
    }
}

/**
 * A (chromosome, offset) pair.
 *
 * @author Sergei Lebedev
 * @since 16/03/15
 */
data class RTreeOffset(
        /** Chromosome ID as defined by the B+ index.  */
        public val chromIx: Int,
        /** 0-based genomic offset.  */
        public val offset: Int) : Comparable<RTreeOffset> {

    override fun compareTo(other: RTreeOffset): Int = ComparisonChain.start()
            .compare(chromIx, other.chromIx)
            .compare(offset, other.offset)
            .result()

    override fun toString(): String = "$chromIx:$offset"
}

