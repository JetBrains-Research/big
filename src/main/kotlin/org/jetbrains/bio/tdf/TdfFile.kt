package org.jetbrains.bio.tdf

import org.apache.log4j.Logger
import org.jetbrains.bio.*
import java.io.Closeable
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.*

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
 */
class TdfFile @Throws(IOException::class) private constructor(val path: Path) :
        Closeable, AutoCloseable {

    private val input = SeekableDataInput.of(path)
    private val index: TdfMasterIndex
    private val header: Header = Header.read(input)

    val windowFunctions = input.readSequenceOf { WindowFunction.read(this) }.toList()
    val trackType = TrackType.read(input)
    val trackLine = input.readCString().trim()
    val trackNames = input.readSequenceOf { readCString() }.toList()
    val build = input.readCString()
    val compressed = (input.readInt() and 0x1) != 0
    val version: Int get() = header.version

    init {
        // Make sure we haven't read anything extra.
        check(input.tell() == header.headerSize.toLong() + Header.BYTES)
        index = input.with(header.indexOffset, header.indexSize.toLong()) {
            TdfMasterIndex.read(this)
        }
    }

    val dataSetNames: Set<String> get() = index.datasets.keys

    val groupNames: Set<String> get() = index.groups.keys

    /**
     * Returns a list of dataset tiles overlapping a given interval.
     *
     * @param dataset dataset to query.
     * @param startOffset 0-based start offset.
     * @param endOffset 0-based end offset.
     * @return a list of tiles.
     */
    fun query(dataset: TdfDataset, startOffset: Int, endOffset: Int): List<TdfTile> {
        val startTile = startOffset / dataset.tileWidth
        val endTile = endOffset divCeiling dataset.tileWidth
        return (startTile..Math.min(dataset.tileCount - 1, endTile)).map {
            getTile(dataset, it)
        }.filterNotNull()
    }

    /**
     * The implementation is thread safe, no additional synchronization
     * is required.
     */
    fun summarize(chromosome: String, startOffset: Int, endOffset: Int,
                  zoom: Int = 0): TdfSummary {
        val dataset = try {
            getDataset(chromosome, zoom)
        } catch (e: NoSuchElementException) {
            // TODO: we can do better here, taking into account bin size,
            // used when computing zooms. By definition there are 2^z tiles
            // per chromosome, and 700 bins per tile, where z is the zoom
            // level.
            getDatasetInternal("/$chromosome/raw")
        }

        return TdfSummary(query(dataset, startOffset, endOffset),
                          startOffset, endOffset)
    }

    // XXX ideally this should be part of 'TdfDataset', but it's unclear
    //     how to share resources between the dataset and 'TdfFile'.
    fun getTile(ds: TdfDataset, tileNumber: Int): TdfTile? {
        return with(ds) {
            require(tileNumber >= 0 && tileNumber < tileCount) { "invalid tile index" }
            val position = tilePositions[tileNumber]
            if (position < 0) {
                return null  // Indicates empty tile.
            }

            synchronized(input) {
                input.with(position, tileSizes[tileNumber].toLong(),
                           compressed = compressed) {
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
        synchronized(input) {
            return input.with(offset, size.toLong()) { TdfDataset.read(this) }
        }
    }

    fun getGroup(name: String): TdfGroup {
        if (name !in index.groups) {
            throw NoSuchElementException(name)
        }

        val (offset, size) = index.groups[name]!!
        synchronized(input) {
            return input.with(offset, size.toLong()) { TdfGroup.read(this) }
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
            val BYTES = 24

            internal fun read(input: SeekableDataInput) = with(input) {
                val b = ByteArray(4)
                readFully(b)
                order = ByteOrder.LITTLE_ENDIAN
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

    @Throws(IOException::class)
    override fun close() = input.close()

    companion object {
        private val LOG = Logger.getLogger(TdfFile::class.java)

        @Throws(IOException::class)
        @JvmStatic fun read(path: Path) = TdfFile(path)
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
internal data class TdfMasterIndex private constructor(
        val datasets: Map<String, IndexEntry>,
        val groups: Map<String, IndexEntry>) {

    companion object {
        private fun OrderedDataInput.readIndex(): Map<String, IndexEntry> {
            return readSequenceOf {
                val name = readCString()
                val fPosition = readLong()
                val n = readInt()
                name to IndexEntry(fPosition, n)
            }.toMap()
        }

        fun read(input: OrderedDataInput) = with(input) {
            val datasets = readIndex()
            val groups = readIndex()
            TdfMasterIndex(datasets, groups)
        }
    }
}

private fun <T> OrderedDataInput.readSequenceOf(
        block: OrderedDataInput.() -> T): Sequence<T> {
    return (0 until readInt()).mapUnboxed { block() }
}

private fun OrderedDataInput.readAttributes(): Map<String, String> {
    return readSequenceOf {
        val key = readCString()
        val value = readCString()
        key to value
    }.toMap()
}

/**
 * Dataset wraps a number of tiles aka data-containers.
 *
 * In theory dataset is abstract wrt to the data types stored
 * in the tiles, but IGV implementation seems to always use
 * floats.
 */
data class TdfDataset private constructor(
        val attributes: Map<String, String>,
        val tileWidth: Int, val tileCount: Int,
        val tilePositions: LongArray, val tileSizes: IntArray) {

    companion object {
        fun read(input: OrderedDataInput) = with(input) {
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
 * The data container.
 */
interface TdfTile {
    fun view(trackNumber: Int) = TdfTileView(this, trackNumber)

    fun getValue(trackNumber: Int, idx: Int): Float

    // TODO: position implies 1-based indexing, which isn't the case.
    fun getStartPosition(idx: Int): Int

    fun getEndPosition(idx: Int): Int

    /** Number of data points in a tile. */
    val size: Int

    companion object {
        private val LOG = Logger.getLogger(TdfTile::class.java)

        internal fun read(input: OrderedDataInput, expectedTracks: Int) = with(input) {
            val type = readCString()
            when (type) {
                "fixedStep" -> TdfFixedTile.fill(this, expectedTracks)
                "variableStep" -> TdfVaryTile.fill(this, expectedTracks)
                "bed", "bedWithName" -> {
                    if (type === "bedWithName") {
                        LOG.warn("bedWithName is not supported, assuming bed")
                    }

                    TdfBedTile.fill(this, expectedTracks)
                }
                else -> error("unexpected type: $type")
            }
        }
    }
}

class TdfTileView(private val tile: TdfTile,
                  private val trackNumber: Int) : Iterable<ScoredInterval?> {
    override fun iterator(): Iterator<ScoredInterval?> {
        return (0 until tile.size).asSequence().map {
            val value = tile.getValue(trackNumber, it)
            if (value.isNaN()) {
                null
            } else {
                val start = tile.getStartPosition(it)
                val end = tile.getEndPosition(it)
                ScoredInterval(start, end, value)
            }
        }.iterator()
    }
}

data class TdfBedTile(val starts: IntArray, val ends: IntArray,
                      val data: Array<FloatArray>) : TdfTile {
    override val size: Int get() = starts.size

    override fun getStartPosition(idx: Int) = starts[idx]

    override fun getEndPosition(idx: Int) = ends[idx]

    override fun getValue(trackNumber: Int, idx: Int) = data[trackNumber][idx]

    companion object {
        fun fill(input: OrderedDataInput, expectedTracks: Int) = with(input) {
            val size = readInt()
            val start = IntArray(size)
            for (i in 0 until size) {
                start[i] = readInt()
            }
            val end = IntArray(size)
            for (i in 0 until size) {
                end[i] = readInt()
            }

            val trackCount = readInt()
            check(trackCount == expectedTracks) {
                "expected $expectedTracks tracks, got: $trackCount"
            }
            val data = Array(trackCount) {
                val acc = FloatArray(size)
                for (i in 0 until size) {
                    acc[i] = readFloat()
                }
                acc
            }

            TdfBedTile(start, end, data)
        }
    }
}

data class TdfFixedTile(val start: Int, val span: Double, val data: Array<FloatArray>) : TdfTile {

    override val size: Int get() = data.first().size

    override fun getStartPosition(idx: Int): Int {
        return start + (idx * span).toInt()
    }

    override fun getEndPosition(idx: Int): Int {
        return start + ((idx + 1) * span).toInt()
    }

    override fun getValue(trackNumber: Int, idx: Int) = data[trackNumber][idx]

    companion object {
        fun fill(input: OrderedDataInput, expectedTracks: Int) = with(input) {
            val size = readInt()
            val start = readInt()
            val span = readFloat().toDouble()

            // vvv not part of the implementation, see igvteam/igv/#180.
            // val trackCount = readInt()
            val data = Array(expectedTracks) {
                val acc = FloatArray(size)
                for (i in 0 until size) {
                    acc[i] = readFloat()
                }
                acc
            }

            TdfFixedTile(start, span, data)
        }
    }
}

data class TdfVaryTile(val starts: IntArray, val span: Int,
                       val data: Array<FloatArray>) : TdfTile {

    override val size: Int get() = starts.size

    override fun getStartPosition(idx: Int) = starts[idx]

    override fun getEndPosition(idx: Int) = (starts[idx] + span).toInt()

    override fun getValue(trackNumber: Int, idx: Int) = data[trackNumber][idx]

    companion object {
        fun fill(input: OrderedDataInput, expectedTracks: Int) = with(input) {
            // This is called 'tiledStart' in IGV sources and is unused.
            val start = readInt()
            val span = readFloat().toInt()  // Really?
            val size = readInt()

            val step = IntArray(size)
            for (i in 0 until size) {
                step[i] = readInt()
            }

            val trackCount = readInt()
            check(trackCount == expectedTracks) {
                "expected $expectedTracks tracks, got: $trackCount"
            }
            val data = Array(trackCount) {
                val acc = FloatArray(size)
                for (i in 0 until size) {
                    acc[i] = readFloat()
                }

                acc
            }

            TdfVaryTile(step, span, data)
        }
    }
}


/**
 * A group is just a container of key-value attributes.
 */
data class TdfGroup(val attributes: Map<String, String>) {
    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            TdfGroup(readAttributes())
        }
    }

    operator fun get(name: String) = attributes[name]
}

enum class WindowFunction {
    MEAN;

    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            valueOf(readCString().toUpperCase())
        }
    }
}

data class TrackType(val id: String) {
    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            TrackType(readCString())
        }
    }
}