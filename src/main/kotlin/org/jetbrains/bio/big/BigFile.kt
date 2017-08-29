package org.jetbrains.bio.big

import com.google.common.collect.Iterators
import com.google.common.primitives.Ints
import com.indeed.util.mmap.MMapBuffer
import gnu.trove.TCollections
import gnu.trove.map.TIntObjectMap
import gnu.trove.map.hash.TIntObjectHashMap
import org.apache.log4j.LogManager
import org.jetbrains.bio.*
import java.io.Closeable
import java.io.FileInputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.LazyThreadSafetyMode.NONE

/**
 * A common superclass for Big files.
 *
 * Supported format versions
 *
 *   3  full support
 *   4  partial support, specifically, extra indices aren't supported
 *   5  custom version, requires Snappy instead of DEFLATE for
 *      compressed data blocks
 */
abstract class BigFile<T> internal constructor(
        internal val memBuff: MMapBuffer,
        internal val header: Header,
        internal val zoomLevels: List<ZoomLevel>,
        internal val bPlusTree: BPlusTree,
        internal val rTree: RTreeIndex) : Closeable, AutoCloseable {

    /** Whole-file summary. */
    val totalSummary: BigSummary by lazy(NONE) {
        BigSummary.read(MMBRomBuffer(memBuff), header.totalSummaryOffset)
    }

    /**
     * An in-memory mapping of chromosome IDs to chromosome names.
     *
     * Because sometimes (always) you don't need a B+ tree for that.
     */
    val chromosomes: TIntObjectMap<String> by lazy(NONE) {
        with(bPlusTree) {
            val res = TIntObjectHashMap<String>(header.itemCount)
            for ((key, id) in traverse(MMBRomBuffer(memBuff))) {
                res.put(id, key)
            }

            TCollections.unmodifiableMap(res)
        }
    }

    /**
     * File compression type.
     *
     * @since 0.2.6
     */
    val compression: CompressionType get() = with(header) {
        when {
            // Compression was introduced in version 3 of the format. See
            // bbiFile.h in UCSC sources.
            version < 3 || uncompressBufSize == 0 -> CompressionType.NO_COMPRESSION
            version <= 4 -> CompressionType.DEFLATE
            version == 5 -> CompressionType.SNAPPY
            else -> error("unsupported version: $version")
        }
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
     * @param numBins number of summaries to compute. Defaults to `1`.
     * @param index if `true` pre-computed is index is used if possible.
     * @return a list of summaries.
     */
    @Throws(IOException::class)
    fun summarize(name: String, startOffset: Int = 0, endOffset: Int = 0,
                  numBins: Int = 1, index: Boolean = true): List<BigSummary> {

        val input = MMBRomBuffer(memBuff)
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
            LOG.trace("Summarizing $query from raw data")
            summarizeInternal(input, query, numBins)
        } else {
            LOG.trace("Summarizing $query from ${zoomLevel.reduction}x zoom")
            summarizeFromZoom(input, query, zoomLevel, numBins)
        }

        val emptySummary = BigSummary()
        val summaries = Array(numBins) { emptySummary }
        for ((i, summary) in sparseSummaries) {
            summaries[i] = summary
        }

        return summaries.asList()
    }

    @Throws(IOException::class)
    internal abstract fun summarizeInternal(
            input: MMBRomBuffer,
            query: ChromosomeInterval,
            numBins: Int): Sequence<IndexedValue<BigSummary>>

    private fun summarizeFromZoom(input: MMBRomBuffer,
                                  query: ChromosomeInterval, zoomLevel: ZoomLevel,
                                  numBins: Int): Sequence<IndexedValue<BigSummary>> {
        val zRTree = RTreeIndex.read(input, zoomLevel.indexOffset)
        val zoomData = zRTree.findOverlappingBlocks(input, query).flatMap { block ->
            assert(!compression.absent || block.dataSize % ZoomData.SIZE == 0L)
            input.with(block.dataOffset, block.dataSize, compression) {
                val res = ArrayList<ZoomData>()
                do {
                    val zoomData = ZoomData.read(this)
                    if (zoomData.interval intersects query) {
                        res.add(zoomData)
                    }
                } while (hasRemaining())

                res.asSequence()
            }

            // XXX we can avoid explicit '#toList' call here, but the
            // worst-case space complexity will still be O(n).
        }.toList()

        var edge = 0  // yay! map with a side effect.
        return query.slice(numBins).mapIndexed { i, bin ->
            val summary = BigSummary()
            for (j in edge..zoomData.size - 1) {
                val interval = zoomData[j].interval
                if (interval.endOffset <= bin.startOffset) {
                    edge = j + 1
                    continue
                } else if (interval.startOffset > bin.endOffset) {
                    break
                }

                if (interval intersects bin) {
                    summary.update(zoomData[j],
                                   interval.intersectionLength(bin),
                                   interval.length())
                }
            }

            if (summary.isEmpty()) null else IndexedValue(i, summary)
        }.filterNotNull()
    }

    /**
     * Queries an R+-tree.
     *
     * @param name human-readable chromosome name, e.g. `"chr9"`.
     * @param startOffset 0-based start offset (inclusive).
     * @param endOffset 0-based end offset (exclusive), if 0 than the whole
     *                  chromosome is used.
     * @param overlaps if `false` the resulting list contains only the
     *                 items completely contained within the query,
     *                 otherwise it also includes the items overlapping
     *                 the query.
     * @return a list of items.
     * @throws IOException if the underlying [RomBuffer] does so.
     */
    @Throws(IOException::class)
    @JvmOverloads fun query(name: String, startOffset: Int = 0, endOffset: Int = 0,
                            overlaps: Boolean = false): Sequence<T> {
        val input = MMBRomBuffer(memBuff)
        val res = bPlusTree.find(input, name)
        return if (res == null) {
            emptySequence()
        } else {
            val (_key, chromIx, size) = res
            val properEndOffset = if (endOffset == 0) size else endOffset
            query(input, Interval(chromIx, startOffset, properEndOffset), overlaps)
        }
    }

    internal fun query(input: MMBRomBuffer, query: ChromosomeInterval, overlaps: Boolean): Sequence<T> {
        return rTree.findOverlappingBlocks(input, query)
                .flatMap { queryInternal(input, it.dataOffset, it.dataSize, query, overlaps) }
    }

    @Throws(IOException::class)
    internal abstract fun queryInternal(input: MMBRomBuffer,
                                        dataOffset: Long, dataSize: Long,
                                        query: ChromosomeInterval,
                                        overlaps: Boolean): Sequence<T>

    override fun close() {}

    internal data class Header(val order: ByteOrder, val magic: Int, val version: Int = 5,
                               val zoomLevelCount: Int = 0,
                               val chromTreeOffset: Long, val unzoomedDataOffset: Long,
                               val unzoomedIndexOffset: Long, val fieldCount: Int,
                               val definedFieldCount: Int, val asOffset: Long = 0,
                               val totalSummaryOffset: Long = 0, val uncompressBufSize: Int,
                               val extendedHeaderOffset: Long = 0) {

        internal fun write(output: OrderedDataOutput) = with(output) {
            check(output.tell() == 0L)  // a header is always first.
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
            internal val BYTES = 64

            internal fun read(input: MMBRomBuffer, magic: Int) = with(input) {
                checkHeader(magic)

                val version = getUnsignedShort()
                val zoomLevelCount = getUnsignedShort()
                val chromTreeOffset = getLong()
                val unzoomedDataOffset = getLong()
                val unzoomedIndexOffset = getLong()
                val fieldCount = getUnsignedShort()
                val definedFieldCount = getUnsignedShort()
                val asOffset = getLong()
                val totalSummaryOffset = getLong()
                val uncompressBufSize = getInt()
                val extendedHeaderOffset = getLong()

                when {
                    asOffset > 0 -> LOG.warn("AutoSQL queries are unsupported")
                    extendedHeaderOffset > 0 -> LOG.warn("Header extensions are unsupported")
                }

                Header(order, magic, version, zoomLevelCount, chromTreeOffset,
                       unzoomedDataOffset, unzoomedIndexOffset,
                       fieldCount, definedFieldCount, asOffset,
                       totalSummaryOffset, uncompressBufSize,
                       extendedHeaderOffset)
            }
        }
    }

    companion object {
        private val LOG = LogManager.getLogger(BigFile::class.java)

        /**
         * Magic specifies file format and bytes order. Let's read magic as little endian.
         */
        private fun readLEMagic(path: Path): Int {
            FileInputStream(path.toFile()).channel.use { fc ->
                val buf = ByteBuffer.allocate(Ints.BYTES)
                buf.order(ByteOrder.LITTLE_ENDIAN)
                fc.read(buf)
                buf.position(0)
                return buf.int
            }
        }

        /**
         * Determines byte order using expected magic field value
         */
        internal fun getByteOrder(path: Path, magic: Int): ByteOrder {
            val leMagic = readLEMagic(path)
            val (valid, byteOrder) = guess(magic, leMagic)
            check(valid) {
                val bigMagic = java.lang.Integer.reverseBytes(magic)
                "Unexpected header leMagic: Actual $leMagic doesn't match expected LE=$magic and BE=$bigMagic}"
            }
            return byteOrder
        }

        private fun guess(expectedMagic: Int, littleEndianMagic: Int): Pair<Boolean, ByteOrder> {
            if (littleEndianMagic != expectedMagic) {
                val bigEndianMagic = java.lang.Integer.reverseBytes(littleEndianMagic)
                if (bigEndianMagic != expectedMagic) {
                    return false to ByteOrder.BIG_ENDIAN
                }
                return (true to ByteOrder.BIG_ENDIAN)
            }

            return true to ByteOrder.LITTLE_ENDIAN
        }

        fun read(path: Path): BigFile<out Comparable<*>> {
            val magic = readLEMagic(path)
            return when {
                guess(BigBedFile.MAGIC, magic).first -> BigBedFile.read(path)
                guess(BigWigFile.MAGIC, magic).first -> BigWigFile.read(path)
                else -> throw IllegalStateException("Unsupported file header magic: $magic")
            }
        }
    }

    /** Ad hoc post-processing for [BigFile]. */
    protected object Post {
        private val LOG = LogManager.getLogger(javaClass)

        private inline fun <T>  modify(path: Path, offset: Long = 0L,
                                       block: (BigFile<*>, OrderedDataOutput) -> T): T {
            val bf = read(path)
            return try {
                OrderedDataOutput(path, bf.header.order, offset, create = false).use {
                    block(bf, it)
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
         * @param itemsPerSlot number of summaries to aggregate prior to
         *                     building an R+ tree. Lower values allow
         *                     for better granularity when loading zoom
         *                     levels, but may degrade the performance
         *                     of [BigFile.summarizeFromZoom]. See
         *                     [RTreeIndex] for details
         * @param initial initial reduction.
         * @param step reduction step to use, i.e. the first zoom level
         *             will be `initial`, next `initial * step` etc.
         */
        internal fun zoom(path: Path, itemsPerSlot: Int = 512,
                          initial: Int = 8, step: Int = 4) {
            LOG.time("Computing zoom levels with step $step for $path") {
                val zoomLevels = ArrayList<ZoomLevel>()
                modify(path, offset = Files.size(path)) { bf, output ->
                    var reduction = initial
                    for (level in 0..bf.zoomLevels.size - 1) {
                        val zoomLevel = reduction.zoomAt(bf, output, itemsPerSlot)
                        if (zoomLevel == null) {
                            LOG.trace("${reduction}x reduction rejected")
                            break
                        } else {
                            LOG.trace("${reduction}x reduction accepted")
                            zoomLevels.add(zoomLevel)
                        }

                        reduction *= step
                    }
                }

                modify(path, offset = Header.BYTES.toLong()) { bf, output ->
                    for (zoomLevel in zoomLevels) {
                        val zoomHeaderOffset = output.tell()
                        zoomLevel.write(output)
                        assert((output.tell() - zoomHeaderOffset).toInt() == ZoomLevel.BYTES)
                    }
                }
            }
        }

        private fun Int.zoomAt(bf: BigFile<*>, output: OrderedDataOutput,
                               itemsPerSlot: Int): ZoomLevel? {
            val reduction = this
            val zoomedDataOffset = output.tell()
            val leaves = ArrayList<RTreeIndexLeaf>()
            val input = MMBRomBuffer(bf.memBuff)
            for ((name, chromIx, size) in bf.bPlusTree.traverse(input)) {
                val query = Interval(chromIx, 0, size)

                // We can re-use pre-computed zooms, but preliminary
                // results suggest this doesn't give a noticeable speedup.
                val summaries
                        = bf.summarizeInternal(input, query, numBins = size divCeiling reduction)
                for (slot in Iterators.partition(summaries.iterator(), itemsPerSlot)) {
                    val dataOffset = output.tell()
                    output.with(bf.compression) {
                        for ((i, summary) in slot) {
                            val startOffset = i * reduction
                            val endOffset = (i + 1) * reduction
                            val (count, minValue, maxValue, sum, sumSquares) = summary
                            ZoomData(chromIx, startOffset, endOffset,
                                     count.toInt(),
                                     minValue.toFloat(), maxValue.toFloat(),
                                     sum.toFloat(), sumSquares.toFloat()).write(this)
                        }
                    }

                    // Compute the bounding interval.
                    val interval = Interval(chromIx, slot.first().index * reduction,
                                            (slot.last().index + 1) * reduction)
                    leaves.add(RTreeIndexLeaf(interval, dataOffset, output.tell() - dataOffset))
                }
            }

            return if (leaves.size > 1) {
                val zoomedIndexOffset = output.tell()
                RTreeIndex.write(output, leaves, itemsPerSlot = itemsPerSlot)
                ZoomLevel(reduction, zoomedDataOffset, zoomedIndexOffset)
            } else {
                null  // no need for trivial zoom levels.
            }
        }

        /**
         * Fills in whole-file [BigSummary] for a [BigFile] located at [path].
         *
         * The file must contain `BigSummary.BYTES` zero bytes right after
         * the zoom levels block.
         */
        internal fun totalSummary(path: Path) {
            val totalSummaryOffset = BigFile.read(path).use {
                it.header.totalSummaryOffset
            }

            LOG.time("Computing total summary block for $path") {
                modify(path, offset = totalSummaryOffset) { bf, output ->
                    bf.chromosomes.valueCollection()
                            .flatMap { bf.summarize(it, 0, 0, numBins = 1) }
                            .fold(BigSummary(), BigSummary::plus)
                            .write(output)
                    assert((output.tell() - totalSummaryOffset).toInt() == BigSummary.BYTES)
                }
            }
        }
    }
}
