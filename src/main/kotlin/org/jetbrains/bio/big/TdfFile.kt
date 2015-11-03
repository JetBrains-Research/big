package org.jetbrains.bio.big

import com.google.common.primitives.Ints
import java.io.Closeable
import java.io.IOException
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
class TdfFile @Throws(IOException::class) private constructor(path: Path) :
        Closeable, AutoCloseable {

    internal val input = SeekableDataInput.of(path)
    internal val header = Header.read(input, MAGIC)
    internal val windowFunctions = input.readSequenceOf { WindowFunction.read(this) }.toList()
    internal val trackType = TrackType.read(input)
    internal val trackLine = input.readCString().trim()
    internal val trackNames = input.readSequenceOf { readCString() }.toList()
    internal val build = input.readCString()
    internal val compressed = (input.readInt() and 0x1) != 0

    internal val index: TdfMasterIndex

    init {
        // Make sure we haven't read anything extra.
        check(input.tell() == header.headerSize.toLong() + Header.BYTES)

        index = input.with(header.indexOffset, header.indexSize.toLong()) {
            TdfMasterIndex.read(this)
        }
    }

    fun getDataset(chromosome: String, zoom: Int = 0,
                   windowFunction: WindowFunction = WindowFunction.MEAN): TdfDataset {
        require(windowFunction in windowFunctions)
        val name = "/$chromosome/z$zoom/${windowFunction.name.toLowerCase()}"
        if (name !in index.datasets) {
            throw NoSuchElementException(name)
        }

        val (offset, size) = index.datasets[name]!!
        return input.with(offset, size.toLong()) { TdfDataset.read(this) }
    }

    fun getGroup(name: String): TdfGroup {
        if (name !in index.groups) {
            throw NoSuchElementException(name)
        }

        val (offset, size) = index.groups[name]!!
        return input.with(offset, size.toLong()) { TdfGroup.read(this) }
    }

    // XXX ideally this should be part of 'TdfDataset', but it's unclear
    //     how to share resources between the dataset and 'TdfFile'.
    fun getTile(dataset: TdfDataset, idx: Int): TdfTile {
        return with(dataset) {
            require(idx >= 0 && idx < tileCount) { "invalid tile index" }
            input.with(tileOffsets[idx], tileSizes[idx].toLong(),
                       compressed = compressed) {
                TdfTile.read(this, trackNames.size)
            }
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
    internal data class Header(val magic: Int, val version: Int,
                               val indexOffset: Long, val indexSize: Int,
                               val headerSize: Int) {
        companion object {
            /** Number of bytes used for this header. */
            val BYTES = 24

            internal fun read(input: SeekableDataInput, magic: Int) = with(input) {
                guess(magic)

                val version = readInt()
                val indexOffset = readLong()
                val indexSize = readInt()
                val headerSize = readInt()
                Header(magic, version, indexOffset, indexSize, headerSize)
            }
        }
    }

    @Throws(IOException::class)
    override fun close() = input.close()

    companion object {
        /** Unlike UCSC formats, TDF uses little-endian magic. */
        internal val MAGIC = Ints.fromBytes(
                '4'.toByte(), 'F'.toByte(), 'D'.toByte(), 'T'.toByte())

        @Throws(IOException::class)
        @JvmStatic fun read(path: Path) = TdfFile(path)
    }
}

internal data class TdfIndexEntry(val offset: Long, val size: Int)

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
        val datasets: Map<String, TdfIndexEntry>,
        val groups: Map<String, TdfIndexEntry>) {

    companion object {
        private fun OrderedDataInput.readIndex(): Map<String, TdfIndexEntry> {
            return readSequenceOf {
                val name = readCString()
                val offset = readLong()
                val size = readInt()
                name to TdfIndexEntry(offset, size)
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
        val tileOffsets: LongArray, val tileSizes: IntArray) {

    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            val attributes = readAttributes()
            val dataType = readCString()
            check(dataType.toLowerCase() == "float") {
                "unsupported data type: $dataType"
            }
            val tileWidth = readFloat().toInt()
            // ^^^ unsure why this is a float.

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
    companion object {
        fun read(input: OrderedDataInput, trackCount: Int) = with(input) {
            val type = readCString()
            when (type) {
                "fixedStep" -> FixedTdfTile.read(this, trackCount)
                "variableStep" -> VariableTdfTile.read(this)
                "bed" -> BedTdfTile.read(this)
                "bedWithName" -> NamedBedTdfTile.read(this)
                else -> error("unexpected type: $type")
            }
        }
    }
}

data class FixedTdfTile(val start: Int, val step: Int, val span: Int,
                        val data: Array<FloatArray>) : TdfTile {
    companion object {
        fun read(input: OrderedDataInput, trackCount: Int) = with(input) {
            val size = readInt()
            val start = readInt()
            val span = readFloat().toInt()  // Really?
            val step = readInt()

            // vvv not part of the implementation, see igvteam/igv/#180.
            // val trackCount = readInt()
            val data = Array(trackCount) {
                val acc = FloatArray(size)
                for (i in 0 until size) {
                    acc[i] = readFloat()
                }

                acc
            }

            FixedTdfTile(start, step, span, data)
        }
    }
}

data class VariableTdfTile(val start: Int, val step: IntArray, val span: Int,
                           val data: Array<FloatArray>) : TdfTile {
    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            val start = readInt()
            val span = readFloat().toInt()  // Really?
            val size = readInt()
            val step = IntArray(size)
            for (i in 0 until size) {
                step[i] = readInt()
            }

            val trackCount = readInt()
            val data = Array(trackCount) {
                val acc = FloatArray(size)
                for (i in 0 until size) {
                    acc[i] = readFloat()
                }

                acc
            }

            VariableTdfTile(start, step, span, data)
        }
    }
}

data class BedTdfTile(val start: IntArray, val end: IntArray,
                      val data: Array<FloatArray>) : TdfTile {
    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            val size = readInt()
            val start = IntArray(size)
            for (i in 0 until size) {
                start[i] = readInt()
            }
            val end = IntArray(size)
            for (i in 0 until size) {
                start[i] = readInt()
            }

            val trackCount = readInt()
            val data = Array(trackCount) {
                val acc = FloatArray(size)
                for (i in 0 until size) {
                    acc[i] = readFloat()
                }

                acc
            }

            BedTdfTile(start, end, data)
        }
    }
}

data class NamedBedTdfTile(val start: IntArray,
                           val end: IntArray,
                           val names: Array<String>,
                           val data: Array<FloatArray>) : TdfTile {
    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            val (start, end, data) = BedTdfTile.read(this)
            val names = Array(start.size) { readCString() }
            NamedBedTdfTile(start, end, names, data)
        }
    }
}

/**
 * A group is just a container of key-value attributes.
 *
 * TODO: document / group?
 */
data class TdfGroup(val attributes: Map<String, String>) {
    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            TdfGroup(readAttributes())
        }
    }
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