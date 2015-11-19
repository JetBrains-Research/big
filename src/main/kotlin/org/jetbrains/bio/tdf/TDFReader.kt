package org.jetbrains.bio.tdf

import org.apache.log4j.Logger
import org.jetbrains.bio.OrderedDataInput
import org.jetbrains.bio.SeekableDataInput
import org.jetbrains.bio.mapUnboxed
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
class TDFReader @Throws(IOException::class) private constructor(val path: Path) : Closeable, AutoCloseable {

    private val input = SeekableDataInput.of(path)
    private val index: TDFMasterIndex
    private val header: Header

    val windowFunctions: List<WindowFunction>
    val trackType: TrackType
    val trackLine: String
    val trackNames: List<String>
    val build: String
    val compressed: Boolean
    val version: Int

    init {
        header = Header.read(input)
        version = header.version
        windowFunctions = input.readSequenceOf { WindowFunction.read(this) }.toList()
        trackType = TrackType.read(input)
        trackLine = input.readCString().trim()
        trackNames = input.readSequenceOf { readCString() }.toList()
        build = input.readCString()
        compressed = (input.readInt() and 0x1) != 0
        // Make sure we haven't read anything extra.
        check(input.tell() == header.headerSize.toLong() + Header.BYTES)
        index = input.with(header.indexOffset, header.indexSize.toLong()) {
            TDFMasterIndex.read(this)
        }
    }

    fun getDatasetNames(): Set<String> = index.datasets.keys

    fun getGroupNames(): Set<String> = index.groups.keys

    @JvmOverloads
    fun getDatasetZoom(chromosome: String, zoom: Int = 0, windowFunction: WindowFunction = WindowFunction.mean): TDFDataset {
        require(windowFunction in windowFunctions)
        return getDataset("/$chromosome/z$zoom/${windowFunction.name.toLowerCase()}")
    }

    fun getDataset(name: String): TDFDataset {
        if (name !in index.datasets) {
            throw NoSuchElementException(name)
        }

        val (offset, size) = index.datasets[name]!!
        synchronized(input) {
            return input.with(offset, size.toLong()) { TDFDataset.read(this) }
        }
    }

    fun getGroup(name: String): TDFGroup {
        if (name !in index.groups) {
            throw NoSuchElementException(name)
        }

        val (offset, size) = index.groups[name]!!
        synchronized(input) {
            return input.with(offset, size.toLong()) { TDFGroup.read(this) }
        }
    }

    // XXX ideally this should be part of 'TdfDataset', but it's unclear
    //     how to share resources between the dataset and 'TdfFile'.
    fun readTile(ds: TDFDataset, tileNumber: Int): TDFTile? {
        return with(ds) {
            require(tileNumber >= 0 && tileNumber < nTiles) { "invalid tile index" }
            val position = tilePositions[tileNumber]
            if (position < 0) {
                // Indicates empty tile
                return null
            }
            val nBytes = tileSizes[tileNumber]
            synchronized(input) {
                input.with(position, nBytes.toLong(), compressed = compressed) {
                    TDFTile.createTile(this, trackNames.size)
                }
            }
        }
    }

    fun getTiles(ds: TDFDataset, startLocation: Int, endLocation: Int): List<TDFTile> {
        val startTile = (startLocation / ds.tileWidth).toInt()
        val endTile = (endLocation / ds.tileWidth).toInt()
        return (startTile..Math.min(ds.nTiles - 1, endTile)).map {
            readTile(ds, it)
        }.filterNotNull().toList()
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
        val LOG = Logger.getLogger(TDFDataSource::class.java)

        @Throws(IOException::class)
        @JvmStatic fun read(path: Path) = TDFReader(path)
    }
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
internal data class TDFMasterIndex private constructor(
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
            TDFMasterIndex(datasets, groups)
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
data class TDFDataset private constructor(
        val attributes: Map<String, String>,
        val tileWidth: Int, val nTiles: Int,
        val tilePositions: LongArray, val tileSizes: IntArray) {

    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            val attributes = readAttributes()
            val dataType = readCString()
            // TODO: see IGV TDFDataset#DataType
            check(dataType.toLowerCase() == "float") {
                "unsupported data type: $dataType"
            }
            // TODO -- change tileWidth to int ?
            val tileWidth = readFloat().toInt()

            val tileCount = readInt()
            val tileOffsets = LongArray(tileCount)
            val tileSizes = IntArray(tileCount)
            for (i in 0 until tileCount) {
                tileOffsets[i] = readLong()
                tileSizes[i] = readInt()
            }

            TDFDataset(attributes, tileWidth, tileCount, tileOffsets, tileSizes)
        }
    }
}

/**
 * The data container.
 */
interface TDFTile {

    companion object {
        fun createTile(input: OrderedDataInput, nSamples: Int) = with(input) {
            val type = readCString()
            when (type) {
                "fixedStep" -> TDFFixedTile.fill(this, nSamples)
                "variableStep" -> TDFVaryTile.fill(this, nSamples)
                "bed",
                "bedWithName" -> TDFBedTile.fill(this, nSamples, type)
                else -> error("unexpected type: $type")
            }
        }
    }

    fun getStart(): IntArray

    fun getEnd(): IntArray

    fun getData(trackNumber: Int): FloatArray

    fun getNames(): Array<String>?

    fun getSize(): Int

    fun getStartPosition(idx: Int): Int

    fun getEndPosition(idx: Int): Int

    fun getValue(row: Int, idx: Int): Float

}

data class TDFBedTile(val starts: IntArray, val ends: IntArray, val data: Array<FloatArray>) : TDFTile {
    companion object {
        fun fill(input: OrderedDataInput, nSamples: Int, type: String) = with(input) {
            val nPositions = readInt()
            val start = IntArray(nPositions)
            for (i in 0 until nPositions) {
                start[i] = readInt()
            }
            val end = IntArray(nPositions)
            for (i in 0 until nPositions) {
                end[i] = readInt()
            }

            val nS = readInt()
            check(nS == nSamples) { "Illegal number of samples, expected: $nSamples, got: $nS" }
            val data = Array(nS) {
                val acc = FloatArray(nPositions)
                for (i in 0 until nPositions) {
                    acc[i] = readFloat()
                }
                acc
            }
            // Optionally read feature names
            if (type === "bedWithName") {
                TDFReader.LOG.error("bedWithName names not supported")
            }

            TDFBedTile(start, end, data)
        }
    }

    override fun getSize(): Int = starts.size

    override fun getStartPosition(idx: Int): Int = starts[idx]

    override fun getEndPosition(idx: Int): Int = ends[idx]

    override fun getValue(row: Int, idx: Int): Float = data[row][idx]

    override fun getStart(): IntArray = starts

    override fun getEnd(): IntArray = ends

    override fun getData(trackNumber: Int): FloatArray = data[trackNumber]

    override fun getNames(): Array<String>? = null
}

data class TDFFixedTile(val start: Int, val span: Double, val data: Array<FloatArray>) : TDFTile {
    companion object {
        fun fill(input: OrderedDataInput, nSamples: Int) = with(input) {
            val nPositions = readInt()
            val start = readInt()
            val span = readFloat().toDouble()

            // vvv not part of the implementation, see igvteam/igv/#180.
            // val trackCount = readInt()
            val data = Array(nSamples) {
                val acc = FloatArray(nPositions)
                for (i in 0 until nPositions) {
                    acc[i] = readFloat()
                }
                acc
            }

            TDFFixedTile(start, span, data)
        }
    }

    override fun getStartPosition(idx: Int): Int {
        return start + (idx * span).toInt()
    }

    override fun getEndPosition(idx: Int): Int {
        return start + ((idx + 1) * span).toInt()
    }

    override fun getValue(row: Int, idx: Int): Float {
        return data[row][idx]
    }

    override fun getSize(): Int {
        return data[0].size
    }

    /**
     * This should never be called, but is provided to satisfy the interface
     * @return
     */
    override fun getStart(): IntArray =
            throw IllegalStateException("This should never be called")

    override fun getEnd(): IntArray =
            throw IllegalStateException("This should never be called")

    override fun getData(trackNumber: Int): FloatArray =
            data[trackNumber]  //To change body of implemented methods use File | Settings | File Templates.

    override fun getNames(): Array<String>? =
            null
}

data class TDFVaryTile(val start: Int, val starts: IntArray, val span: Int,
                       val data: Array<FloatArray>) : TDFTile {
    companion object {
        fun fill(input: OrderedDataInput, nSamples: Int) = with(input) {
            val start = readInt()
            val span = readFloat().toInt()  // Really?
            val size = readInt()

            val step = IntArray(size)
            for (i in 0 until size) {
                step[i] = readInt()
            }

            val nS = readInt()
            check(nS == nSamples) { "Illegal number of samples, expected: $nSamples, got: $nS" }
            val data = Array(nS) {
                val acc = FloatArray(size)
                for (i in 0 until size) {
                    acc[i] = readFloat()
                }

                acc
            }

            TDFVaryTile(start, step, span, data)
        }
    }

    override fun getSize(): Int = starts.size

    override fun getStartPosition(idx: Int): Int = starts[idx]

    override fun getEndPosition(idx: Int): Int = (starts[idx] + span).toInt()

    override fun getValue(row: Int, idx: Int): Float = data[row][idx]

    override fun getStart(): IntArray = starts

    override fun getEnd(): IntArray {
        val end = IntArray(starts.size)
        for (i in end.indices) {
            end[i] = (starts[i] + span).toInt()
        }
        return end
    }

    override fun getData(trackNumber: Int): FloatArray = data[trackNumber]

    override fun getNames(): Array<String>? = null
}


/**
 * A group is just a container of key-value attributes.
 */
data class TDFGroup(val attributes: Map<String, String>) {
    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            TDFGroup(readAttributes())
        }
    }

    operator fun get(name: String) = attributes[name]
}

enum class WindowFunction {
    mean;

    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            valueOf(readCString())
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