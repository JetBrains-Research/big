package org.jetbrains.bio.big

import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import gnu.trove.map.TIntObjectMap
import java.io.Closeable
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Files
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
        // Skip AutoSQL string if any. Note, that even though the format
        // requires offsets for the following sections, their location
        // is fixed.
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
    throws(IOException::class)
    public fun summarize(name: String,
                         startOffset: Int, endOffset: Int,
                         numBins: Int, index: Boolean = true): List<BigSummary> {
        val chromosome = bPlusTree.find(input, name)
                         ?: throw NoSuchElementException(name)

        val properEndOffset = if (endOffset == 0) chromosome.size else endOffset
        val query = Interval(chromosome.id, startOffset, properEndOffset)

        require(numBins <= query.length()) {
            "number of bins must not exceed interval length, got " +
            "$numBins > ${query.length()}"
        }

        // The 2-factor guarantees that we get at least two data points
        // per bin. Otherwise we might not be able to estimate SD.
        val zoomLevel = zoomLevels.pick(query.length() / (2 * numBins))
        val sparseSummaries = if (zoomLevel == null || !index) {
            summarizeInternal(query, numBins)
        } else {
            summarizeFromZoom(query, zoomLevel, numBins)
        }

        val emptySummary = BigSummary()
        val summaries = Array(numBins) { emptySummary }
        for ((i, summary) in sparseSummaries) {
            summaries[i] = summary
        }

        return summaries.toList()
    }

    throws(IOException::class)
    protected abstract fun summarizeInternal(
            query: ChromosomeInterval, numBins: Int): Sequence<Pair<Int, BigSummary>>

    private fun summarizeFromZoom(query: ChromosomeInterval, zoomLevel: ZoomLevel,
                                  numBins: Int): Sequence<Pair<Int, BigSummary>> {
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
        return query.slice(numBins).mapIndexed { i, bin ->
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

            if (count == 0L) {
                null
            } else {
                i to BigSummary(count = count, minValue = min, maxValue = max,
                                sum = sum, sumSquares = sumSquares)
            }
        }.filterNotNull()
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

    data class Header(val order: ByteOrder, val magic: Int, val version: Int = 4,
                      val zoomLevelCount: Int = 0,
                      val chromTreeOffset: Long, val unzoomedDataOffset: Long,
                      val unzoomedIndexOffset: Long, val fieldCount: Int,
                      val definedFieldCount: Int, val asOffset: Long = 0,
                      val totalSummaryOffset: Long = 0, val uncompressBufSize: Int,
                      val extendedHeaderOffset: Long = 0) {

        fun write(output: SeekableDataOutput) = with(output) {
            seek(0L)  // a header is always first.
            writeInt(magic)
            writeUnsignedShort(version)
            writeUnsignedShort(zoomLevelCount)
            writeLong(chromTreeOffset)
            writeLong(unzoomedDataOffset)
            writeLong(unzoomedIndexOffset)
            writeUnsignedShort(fieldCount)
            writeUnsignedShort(definedFieldCount)
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
                return Header(order, magic, version, zoomLevelCount, chromTreeOffset,
                              unzoomedDataOffset, unzoomedIndexOffset,
                              fieldCount, definedFieldCount, asOffset,
                              totalSummaryOffset, uncompressBufSize,
                              extendedHeaderOffset)
            }
        }
    }

    companion object {
        /** Checks if a given `path` starts with a valid `magic`. */
        fun check(path: Path, magic: Int): Boolean {
            return SeekableDataInput.of(path).use { input ->
                try {
                    input.guess(magic)
                    true
                } catch (e: IllegalStateException) {
                    false
                }
            }
        }

        fun read(path: Path): BigFile<*> = when {
            check(path, BigBedFile.MAGIC) -> BigBedFile.read(path)
            check(path, BigWigFile.MAGIC) -> BigWigFile.read(path)
            else -> throw IllegalStateException()
        }
    }

    /** Ad hoc post-processing for [BigFile]. */
    object Post {
        private inline fun modify(
                path: Path, block: (BigFile<*>, SeekableDataOutput) -> Unit) {
            val bf = read(path)
            try {
                SeekableDataOutput.of(path, bf.header.order).use { output ->
                    block(bf, output)
                }
            } finally {
                bf.close()
            }
        }

        /**
         * Fills in zoom levels for a [BigFile] located at [path].
         *
         * Note that the number of zoom levels to compute must be
         * specified in the file [Header]. Additionally the file must
         * contain `ZoomData.BYTES * header.zoomLevelCount` zero
         * bytes right after the header.
         *
         * @param path to [BigFile].
         * @param step reduction step to use, i.e. the first zoom level
         *             will be `step^2`, next `step^3` etc.
         */
        fun zoom(path: Path, step: Int = 8) = modify(path) { bf, output ->
            output.seek(Files.size(path))  // Do NOT change this!

            val zoomLevelCount = bf.zoomLevels.size()
            val chromosomes = bf.bPlusTree.traverse(bf.input).toList()

            var reduction = step * step
            val zoomLevels = ArrayList<ZoomLevel>(zoomLevelCount)
            for (level in 0 until zoomLevelCount) {
                val leaves = ArrayList<RTreeIndexLeaf>()
                val zoomedDataOffset = output.tell()
                var nonEmpty = true
                for ((name, chromIx, size) in chromosomes) {
                    val numBins = size divCeiling reduction
                    val query = Interval(chromIx, 0, size)
                    val dataOffset = output.tell()
                    var itemCount = 0
                    val dataSize = output.with(bf.compressed) {
                        for ((i, summary) in bf.summarizeInternal(query, numBins)) {
                            val bin = Interval(chromIx, i * reduction, (i + 1) * reduction)
                            (bin to summary).toZoomData().write(this)
                            itemCount++
                        }
                    }

                    // Currently we do a single R+ tree leaf per chromosome,
                    // which is far from optimal.
                    nonEmpty = nonEmpty && itemCount > 0
                    if (itemCount > 0) {
                        leaves.add(RTreeIndexLeaf(query, dataOffset, dataSize.toLong()))
                    }
                }

                if (nonEmpty) {
                    val zoomedIndexOffset = output.tell()
                    RTreeIndex.write(output, leaves)
                    zoomLevels.add(ZoomLevel(reduction, zoomedDataOffset, zoomedIndexOffset))
                    reduction *= step
                } else {
                    break  // no need for trivial zoom levels.
                }
            }

            output.seek(Header.BYTES.toLong())
            for (zoomLevel in zoomLevels) {
                val zoomHeaderOffset = output.tell()
                zoomLevel.write(output)
                assert((output.tell() - zoomHeaderOffset).toInt() == ZoomLevel.BYTES)
            }
        }

        /**
         * Fills in whole-file [BigSummary] for a [BigFile] located at [path].
         *
         * The file must contain `BigSummary.BYTES` zero bytes right after
         * the zoom levels block.
         */
        fun totalSummary(path: Path) = modify(path) { bf, output ->
            val totalSummaryOffset = bf.header.totalSummaryOffset
            output.seek(totalSummaryOffset)
            bf.chromosomes.valueCollection()
                    .flatMap { bf.summarize(it, 0, 0, numBins = 1) }
                    .reduceRight { a, b -> a + b }
                    .write(output)
            assert((output.tell() - totalSummaryOffset).toInt() == BigSummary.BYTES)
        }
    }
}