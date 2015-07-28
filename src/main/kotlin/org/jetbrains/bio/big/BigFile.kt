package org.jetbrains.bio.big

import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import gnu.trove.map.TIntObjectMap
import java.io.Closeable
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.*
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
     * @return a list of summaries.
     */
    public fun summarize(name: String,
                         startOffset: Int, endOffset: Int,
                         numBins: Int): List<BigSummary> {
        val chromosome = bPlusTree.find(input, name)
                         ?: throw NoSuchElementException(name)

        // The 2-factor guarantees that we get at least two data points
        // per bin. Otherwise we won't be able to estimate SD.
        val properEndOffset = if (endOffset == 0) chromosome.size else endOffset
        val desiredReduction = (properEndOffset - startOffset) / (2 * numBins)
        val zoomLevel = zoomLevels.pick(desiredReduction)
        return if (zoomLevel == null) {
            summarizeInternal(chromosome, startOffset, endOffset, numBins)
        } else {
            throw UnsupportedOperationException()  // not implemented.
        }
    }

    throws(IOException::class)
    protected abstract fun summarizeInternal(chromosome: BPlusLeaf,
                                             startOffset: Int, endOffset: Int,
                                             numBins: Int): List<BigSummary>

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
            val query = Interval.of(chromIx, startOffset,
                                    if (endOffset == 0) size else endOffset)
            rTree.findOverlappingBlocks(input, query)
                    .flatMap { queryInternal(it.dataOffset, it.dataSize, query) }
        }
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
            writeShort(fieldCount.toInt())
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

    val interval: ChromosomeInterval get() {
        return Interval.of(chromIx, startOffset, endOffset)
    }

    val summary: BigSummary get() {
        return BigSummary(count.toLong(),
                          minValue.toDouble(), maxValue.toDouble(),
                          sum.toDouble(), sumSquares.toDouble())
    }

    companion object {
        fun read(input: SeekableDataInput) = with(input) {
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