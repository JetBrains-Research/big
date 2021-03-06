package org.jetbrains.bio.tdf

import com.google.common.primitives.Ints
import org.jetbrains.bio.*
import org.jetbrains.bio.big.BigFile.Companion.defaultFactory
import org.jetbrains.bio.big.RomBufferFactoryProvider
import java.io.Closeable
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.*
import kotlin.math.min

/**
 * A Tiled Data Format (TDF) reader.
 *
 * TDF format is a binary format for track data designed by IGV team
 * for their browser.
 *
 * TDF lacks an accurate and complete spec, thus the implementation
 * is based on the high-level format overview in `notes.txt` and
 * the sources of IGV itself.
 *
 * See https://github.com/igvteam/igv/blob/master/src/org/broad/igv/tdf/notes.txt.
 * See https://www.broadinstitute.org/software/igv/TDF.
 *
 * @since 0.2.2
 */
data class TdfFile internal constructor(
        val source: String,
        private val buffFactory: RomBufferFactory,
        private val header: Header,
        private val index: TdfMasterIndex,
        /** Window functions supported by the data, e.g. `"mean"`. */
        val windowFunctions: List<WindowFunction>,
        /** Data type, e.g. `"GENE_EXPRESSION"`. */
        val trackType: TrackType,
        /** Track description, see BED format spec for details. */
        val trackLine: String,
        /** Human-readable track names. */
        val trackNames: List<String>,
        /** Genome build, e.g. `"hg38"`. */
        val build: String,
        /** Whether the data sections are compressed with DEFLATE. */
        val compressed: Boolean
) : Closeable {

    val version: Int get() = header.version

    val dataSetNames: Set<String> get() = index.datasets.keys

    val groupNames: Set<String> get() = index.groups.keys

    /**
     * Returns a list of dataset tiles overlapping a given interval.
     *
     * @param dataset dataset to query.
     * @param startOffset 0-based start offset.
     * @param endOffset 0-based end offset.
     * @param cancelledChecker Throw cancelled exception to abort operation
     * @return a list of tiles.
     */
    fun query(
            dataset: TdfDataset,
            startOffset: Int,
            endOffset: Int,
            cancelledChecker: (() -> Unit)? = null
    ): List<TdfTile> {
        val startTile = startOffset / dataset.tileWidth
        val endTile = endOffset divCeiling dataset.tileWidth
        return (startTile..min(dataset.tileCount - 1, endTile)).mapNotNull {
            cancelledChecker?.invoke()
            getTile(dataset, it)
        }
    }

    /**
     * Returns a summary of the data within a given interval.
     *
     * @since 0.2.3
     */
    fun summarize(
            chromosome: String,
            startOffset: Int, endOffset: Int,
            zoom: Int = 0,
            cancelledChecker: (() -> Unit)? = null
    ): TdfSummary {

        val dataset = try {
            getDataset(chromosome, zoom)
        } catch (e: NoSuchElementException) {
            // TODO: we can do better here, taking into account bin size,
            // used when computing zooms. By definition there are 2^z tiles
            // per chromosome, and 700 bins per tile, where z is the zoom
            // level.
            getDatasetInternal("/$chromosome/raw")
        }
        cancelledChecker?.invoke()
        return TdfSummary(
                query(dataset, startOffset, endOffset, cancelledChecker),
                startOffset, endOffset
        )
    }

    // XXX ideally this should be part of 'TdfDataset', but it's unclear
    //     how to share resources between the dataset and 'TdfFile'.
    fun getTile(ds: TdfDataset, tileNumber: Int): TdfTile? {
        return with(ds) {
            require(tileNumber in 0 until tileCount) { "invalid tile index" }
            val position = tilePositions[tileNumber]
            if (position < 0) {
                return null  // Indicates empty tile.
            }

            val compression = if (compressed) {
                CompressionType.DEFLATE
            } else {
                CompressionType.NO_COMPRESSION
            }

            buffFactory.create().use { input ->
                input.with(
                        position, tileSizes[tileNumber].toLong(), compression, uncompressBufSize = 0
                ) {
                    TdfTile.read(this, trackNames.size)
                }
            }
        }
    }

    @JvmOverloads
    fun getDataset(chromosome: String, zoom: Int = 0,
                   windowFunction: WindowFunction = WindowFunction.MEAN): TdfDataset {
        require(windowFunction in windowFunctions)
        return getDatasetInternal("/$chromosome/z$zoom/${windowFunction.name.toLowerCase()}")
    }

    internal fun getDatasetInternal(name: String): TdfDataset {
        if (name !in index.datasets) {
            throw NoSuchElementException(name)
        }

        val (offset, size) = index.datasets[name]!!
        return buffFactory.create().use { input ->
            input.with(offset, size.toLong(), uncompressBufSize = 0) { TdfDataset.read(this) }
        }
    }

    fun getGroup(name: String): TdfGroup {
        if (name !in index.groups) {
            throw NoSuchElementException(name)
        }

        val (offset, size) = index.groups[name]!!
        return buffFactory.create().use { input ->
            input.with(offset, size.toLong(), uncompressBufSize = 0) { TdfGroup.read(this) }
        }
    }

    /**
     * Header consists of a fixed-size 24 byte component and variable-size
     * component with metadata.
     *
     * magic                   int32  'T' 'D' 'F' '4' in LE byte order
     * version                 int32  currently 4
     * master index offset     int64
     * master index size       int32
     * header size             int32  # of bytes in the variable-size component
     * # of window functions   int32
     * [window function name]  null-terminated string (enum)
     * track type              null-terminated string (enum)
     * track line              null-terminated string
     * # of track names        int32
     * [track name]            null-terminated string
     * build                   null-terminated string
     * flags                   int32  carries compression flag `0x1`
     *
     * Here [] mean that the field can be repeated multiple times.
     */
    internal data class Header(val version: Int,
                               val indexOffset: Long,
                               val indexSize: Int,
                               val headerSize: Int) {
        companion object {
            /** Number of bytes used for this header. */
            const val BYTES = 24

            internal fun read(input: RomBuffer) = with(input) {
                val b = readBytes(4)
                val magicString = String(b)
                check (magicString.startsWith("TDF") || magicString.startsWith("IBF")) {
                    "bad signature in $input"
                }

                val version = readInt()
                val indexOffset = readLong()
                val indexSize = readInt()
                val headerSize = readInt()
                Header(version, indexOffset, indexSize, headerSize)
            }
        }
    }

    override fun close() { buffFactory.close() }

    companion object {
        @Throws(IOException::class)
        @JvmStatic
        fun read(path: Path) = read(path.toString()) { p, byteOrder ->
            EndianSynchronizedBufferFactory.create(p, byteOrder)
        }

        @Throws(IOException::class)
        @JvmStatic
        fun read(path: Path, cancelledChecker: (() -> Unit)? = null) = read(path.toString(), cancelledChecker)

        @Throws(IOException::class)
        @JvmStatic
        fun read(src: String,
                 cancelledChecker: (() -> Unit)? = null,
                 factoryProvider: RomBufferFactoryProvider = defaultFactory()
        ): TdfFile {
            val buffFactory = factoryProvider(src, ByteOrder.LITTLE_ENDIAN)
            try {
                buffFactory.create().use { input ->
                    //TODO: investigate IO usage, e.g. for http urls support

                    cancelledChecker?.invoke()
                    val header = Header.read(input)
                    val windowFunctions = input.getSequenceOf { WindowFunction.read(this) }.toList()
                    val trackType = TrackType.read(input)
                    val trackLine = input.readCString().trim()

                    cancelledChecker?.invoke()
                    val trackNames = input.getSequenceOf { input.readCString() }.toList()
                    val build = input.readCString()
                    val compressed = (input.readInt() and 0x1) != 0

                    cancelledChecker?.invoke()
                    // Make sure we haven't read anything extra.
                    check(Ints.checkedCast(input.position) == header.headerSize + Header.BYTES)
                    val index = input.with(
                            header.indexOffset, header.indexSize.toLong(),
                            uncompressBufSize = 0) {
                        TdfMasterIndex.read(this)
                    }

                    cancelledChecker?.invoke()
                    return TdfFile(src, buffFactory, header, index, windowFunctions, trackType,
                            trackLine, trackNames, build, compressed)
                }
            } catch (e: Exception) {
                buffFactory.close()
                throw e
            }
        }
    }
}

class TdfSummary(private val tiles: List<TdfTile>,
                 private val startOffset: Int,
                 private val endOffset: Int) {
    operator fun get(trackNumber: Int) = tiles.asSequence()
            .flatMap { it.view(trackNumber).asSequence() }
            .filterNotNull()
            .filter { startOffset <= it.start && endOffset > it.end }
            .toList()
}

internal data class IndexEntry(val offset: Long, val size: Int)

/**
 * Master index provides random access to datasets and groups.
 *
 * # of datasets    int32
 * [dataset name    null-terminated string
 *  offset          int64
 *  size in bytes]  int32
 * # of groups      int32
 * [group name      null-terminated string
 *  offset          int64
 *  size in bytes]  int32
 *
 * It's perfectly valid to have zero datasets and groups, thus
 * the repeated fields ([] notation) can be empty.
 */
internal data class TdfMasterIndex internal constructor(
        val datasets: Map<String, IndexEntry>,
        val groups: Map<String, IndexEntry>) {

    companion object {
        private fun RomBuffer.readIndex(): Map<String, IndexEntry> {
            return getSequenceOf {
                val name = readCString()
                val fPosition = readLong()
                val n = readInt()
                name to IndexEntry(fPosition, n)
            }.toMap()
        }

        fun read(input: RomBuffer) = with(input) {
            val datasets = readIndex()
            val groups = readIndex()
            TdfMasterIndex(datasets, groups)
        }
    }
}

/**
 * Dataset wraps a number of tiles aka data-containers.
 *
 * In theory dataset is abstract wrt to the data types stored
 * in the tiles, but IGV implementation seems to always use
 * floats.
 */
data class TdfDataset internal constructor(
        val attributes: Map<String, String>,
        val tileWidth: Int, val tileCount: Int,
        val tilePositions: LongArray, val tileSizes: IntArray) {

    companion object {
        fun read(input: RomBuffer) = with(input) {
            val attributes = readAttributes()
            val dataType = readCString()

            check(dataType.toLowerCase() == "float") {
                "unsupported data type: $dataType"
            }

            val tileWidth = readFloat().toInt()

            val tileCount = readInt()
            val tileOffsets = LongArray(tileCount)
            val tileSizes = IntArray(tileCount)
            for (i in 0 until tileCount) {
                tileOffsets[i] = readLong()
                tileSizes[i] = readInt()
            }

            TdfDataset(attributes, tileWidth, tileCount, tileOffsets, tileSizes)
        }
    }
}

/**
 * A container of key-value attributes.
 */
data class TdfGroup(val attributes: Map<String, String>) {
    companion object {
        fun read(input: RomBuffer) = with(input) {
            TdfGroup(readAttributes())
        }
    }

    operator fun get(name: String) = attributes[name]
}

enum class WindowFunction {
    MEAN;

    companion object {
        fun read(input: RomBuffer) = with(input) {
            valueOf(readCString().toUpperCase())
        }
    }
}

data class TrackType(val id: String) {
    companion object {
        fun read(input: RomBuffer) = with(input) {
            TrackType(readCString())
        }
    }
}

private fun <T> RomBuffer.getSequenceOf(
        block: RomBuffer.() -> T): Sequence<T> {
    return (0 until readInt()).mapUnboxed { block() }
}

private fun RomBuffer.readAttributes(): Map<String, String> {
    return getSequenceOf {
        val key = readCString()
        val value = readCString()
        key to value
    }.toMap()
}
