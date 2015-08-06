package org.jetbrains.bio.big

import com.google.common.primitives.Floats
import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import gnu.trove.map.TIntObjectMap
import java.io.Closeable
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.ArrayList
import java.util.NoSuchElementException
import kotlin.properties.Delegates

/**
 * A common superclass for Big files.
 */
abstract class BigFile<T> protected constructor(path: Path, magic: Int) :
        Closeable, AutoCloseable {

    val input: SeekableDataInput = SeekableDataInput.of(path)
    val header: Header = Header.read(input, magic)
    val zoomLevels: List<ZoomLevel> = (0 until header.zoomLevelCount.toInt()).asSequence()
            .map { ZoomLevel.read(input) }.toList()
    val bPlusTree: BPlusTree
    val rTree: RTreeIndex

    init {
        // Skip AutoSQL string if any.
        while (header.asOffset > 0 && input.readByte() != 0.toByte()) {}

        // Skip total summary block.
        if (header.totalSummaryOffset > 0) {
            BigSummary.read(input)
        }

        // Skip extended header. Ideally, we should issue a warning
        // if extensions are present.
        if (header.extendedHeaderOffset > 0) {
            with(input) {
                skipBytes(Shorts.BYTES)  // extensionSize.
                val extraIndexCount = readUnsignedShort()
                skipBytes(Longs.BYTES)   // extraIndexListOffset.
                skipBytes(48)            // reserved.

                for (i in 0 until extraIndexCount) {
                    val type = readUnsignedShort()
                    assert(type == 0)
                    val extraFieldCount = readUnsignedShort()
                    skipBytes(Longs.BYTES)     // indexOffset.
                    skipBytes(extraFieldCount *
                              (Shorts.BYTES +  // fieldId,
                               Shorts.BYTES))  // reserved.
                }
            }
        }

        bPlusTree = BPlusTree.read(input, header.chromTreeOffset)
        check(bPlusTree.header.order == header.order)
        rTree = RTreeIndex.read(input, header.unzoomedIndexOffset)
        check(rTree.header.order == header.order)
    }

    public val chromosomes: TIntObjectMap<String> by Delegates.lazy {
        bPlusTree.toMap(input)
    }

    public val compressed: Boolean get() {
        // Compression was introduced in version 3 of the format. See
        // bbiFile.h in UCSC sources.
        return header.version >= 3 && header.uncompressBufSize > 0
    }

    /**
     * Splits the interval `[startOffset, endOffset)` into `numBins`
     * non-intersecting sub-intervals (aka bins) and computes a summary
     * of the data values for each bin.
     *
     * @param name human-readable chromosome name, e.g. `"chr9"`.
     * @param startOffset 0-based start offset (inclusive).
     * @param endOffset 0-based end offset (exclusive), if 0 than the whole
     *                  chromosome is used.
     * @param numBins number of summaries to compute
     * @param index if `true` pre-computed is index is used if possible.
     * @return a list of summaries.
     */
    public fun summarize(name: String,
                         startOffset: Int, endOffset: Int,
                         numBins: Int, index: Boolean = true): List<BigSummary> {
        val chromosome = bPlusTree.find(input, name)
                         ?: throw NoSuchElementException(name)

        val properEndOffset = if (endOffset == 0) chromosome.size else endOffset
        val query = Interval(chromosome.id, startOffset, properEndOffset)

        // The 2-factor guarantees that we get at least two data points
        // per bin. Otherwise we might not be able to estimate SD.
        val zoomLevel = zoomLevels.pick(query.length() / (2 * numBins))
        return if (zoomLevel == null || !index) {
            summarizeInternal(query, numBins)
        } else {
            summarizeFromZoom(query, zoomLevel, numBins)
        }
    }

    throws(IOException::class)
    protected abstract fun summarizeInternal(query: ChromosomeInterval,
                                             numBins: Int): List<BigSummary>

    private fun summarizeFromZoom(query: ChromosomeInterval, zoomLevel: ZoomLevel,
                                  numBins: Int): List<BigSummary> {
        val zRTree = RTreeIndex.read(input, zoomLevel.indexOffset)
        val zoomData = zRTree.findOverlappingBlocks(input, query).flatMap { block ->
            assert(compressed || block.dataSize % ZoomData.SIZE == 0L)
            input.with(block.dataOffset, block.dataSize, compressed) {
                val res = ArrayList<ZoomData>()
                do {
                    val zoomData = ZoomData.read(this)
                    if (query intersects zoomData.interval) {
                        res.add(zoomData)
                    }
                } while (!finished)

                res.asSequence()
            }

            // XXX we can avoid explicit '#toList' call here, but the
            // worst-case space complexity will still be O(n).
        }.toList()

        var edge = 0  // yay! map with a side effect.
        return query.slice(numBins).map { bin ->
            var count = 0L
            var min = Double.POSITIVE_INFINITY
            var max = Double.NEGATIVE_INFINITY
            var sum = 0.0
            var sumSquares = 0.0
            for (j in edge until zoomData.size()) {
                val interval = zoomData[j].interval
                if (interval.endOffset <= bin.startOffset) {
                    edge = j + 1
                    continue
                } else if (interval.startOffset > bin.endOffset) {
                    break
                }

                if (interval intersects bin) {
                    val intersection = interval intersection  bin
                    assert(intersection.length() > 0)
                    val weight = intersection.length().toDouble() / interval.length()
                    count += Math.round(zoomData[j].count * weight)
                    sum += zoomData[j].sum * weight;
                    sumSquares += zoomData[j].sumSquares * weight
                    min = Math.min(min, zoomData[j].minValue.toDouble());
                    max = Math.max(max, zoomData[j].maxValue.toDouble());
                }
            }

            BigSummary(count = count, minValue = min, maxValue = max,
                       sum = sum, sumSquares = sumSquares)
        }.toList()
    }

    /**
     * Queries an R+-tree.
     *
     * @param name human-readable chromosome name, e.g. `"chr9"`.
     * @param startOffset 0-based start offset (inclusive).
     * @param endOffset 0-based end offset (exclusive), if 0 than the whole
     *                  chromosome is used.
     * @return a list of intervals completely contained within the query.
     * @throws IOException if the underlying [SeekableDataInput] does so.
     */
    throws(IOException::class)
    public fun query(name: String, startOffset: Int, endOffset: Int): Sequence<T> {
        val res = bPlusTree.find(input, name)
        return if (res == null) {
            emptySequence()
        } else {
            val (_key, chromIx, size) = res
            val properEndOffset = if (endOffset == 0) size else endOffset
            query(Interval(chromIx, startOffset, properEndOffset))
        }
    }

    protected fun query(query: ChromosomeInterval): Sequence<T> {
        return rTree.findOverlappingBlocks(input, query)
                .flatMap { queryInternal(it.dataOffset, it.dataSize, query) }
    }

    throws(IOException::class)
    protected abstract fun queryInternal(dataOffset: Long, dataSize: Long,
                                         query: ChromosomeInterval): Sequence<T>

    throws(IOException::class)
    override fun close() = input.close()

    class Header(val order: ByteOrder, val version: Int = 4, val zoomLevelCount: Int = 0,
                 val chromTreeOffset: Long, val unzoomedDataOffset: Long,
                 val unzoomedIndexOffset: Long, val fieldCount: Int,
                 val definedFieldCount: Int, val asOffset: Long = 0,
                 val totalSummaryOffset: Long = 0, val uncompressBufSize: Int,
                 val extendedHeaderOffset: Long = 0) {

        fun write(output: SeekableDataOutput, magic: Int) = with(output) {
            seek(0L)  // a header is always first.
            writeInt(magic)
            writeShort(version)
            writeShort(zoomLevelCount)
            writeLong(chromTreeOffset)
            writeLong(unzoomedDataOffset)
            writeLong(unzoomedIndexOffset)
            writeShort(fieldCount)
            writeShort(definedFieldCount)
            writeLong(asOffset)
            writeLong(totalSummaryOffset)
            writeInt(uncompressBufSize)
            writeLong(extendedHeaderOffset)
        }

        companion object {
            /** Number of bytes used for this header. */
            val BYTES = 64

            fun read(input: SeekableDataInput, magic: Int): Header = with(input) {
                guess(magic)

                val version = readUnsignedShort()
                val zoomLevelCount = readUnsignedShort()
                val chromTreeOffset = readLong()
                val unzoomedDataOffset = readLong()
                val unzoomedIndexOffset = readLong()
                val fieldCount = readUnsignedShort()
                val definedFieldCount = readUnsignedShort()
                val asOffset = readLong()
                val totalSummaryOffset = readLong()
                val uncompressBufSize = readInt()
                val extendedHeaderOffset = readLong()
                return Header(order, version, zoomLevelCount, chromTreeOffset,
                              unzoomedDataOffset, unzoomedIndexOffset,
                              fieldCount, definedFieldCount, asOffset,
                              totalSummaryOffset, uncompressBufSize,
                              extendedHeaderOffset)
            }
        }
    }
}

data class ZoomLevel(public val reductionLevel: Int,
                     public val dataOffset: Long,
                     public val indexOffset: Long) {
    companion object {
        fun read(input: SeekableDataInput) = with(input) {
            val reductionLevel = readInt()
            val reserved = readInt()
            check(reserved == 0)
            val dataOffset = readLong()
            val indexOffset = readLong()
            ZoomLevel(reductionLevel, dataOffset, indexOffset)
        }
    }
}

fun List<ZoomLevel>.pick(desiredReduction: Int): ZoomLevel? {
    require(desiredReduction >= 0, "desired must be >=0")
    return if (desiredReduction <= 1) {
        null
    } else {
        var acc = Int.MAX_VALUE
        var closest: ZoomLevel? = null
        for (zoomLevel in this) {
            val d = desiredReduction - zoomLevel.reductionLevel
            if (d >= 0 && d < acc) {
                acc = d
                closest = zoomLevel
            }
        }

        closest
    }
}

data class ZoomData(
        /** Chromosome id as defined by B+ tree. */
        val chromIx: Int,
        /** 0-based start offset (inclusive). */
        val startOffset: Int,
        /** 0-based end offset (exclusive). */
        val endOffset: Int,
        /**
         * These are just inlined fields of [BigSummary] downcasted
         * to 4 bytes. Top-notch academic design! */
        val count: Int,
        val minValue: Float,
        val maxValue: Float,
        val sum: Float,
        val sumSquares: Float) {

    val interval: ChromosomeInterval get() = Interval(chromIx, startOffset, endOffset)

    companion object {
        val SIZE: Int = Ints.BYTES * 3 +
                        Ints.BYTES + Floats.BYTES * 4

        fun read(input: OrderedDataInput): ZoomData = with(input) {
            val chromIx = readInt()
            val startOffset = readInt()
            val endOffset = readInt()
            val count = readInt()
            val minValue = readFloat()
            val maxValue = readFloat()
            val sum = readFloat();
            val sumSquares = readFloat();
            return ZoomData(chromIx, startOffset, endOffset, count,
                            minValue, maxValue, sum, sumSquares);
        }
    }
}