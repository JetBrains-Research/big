package org.jetbrains.bio.big

import com.google.common.primitives.Ints
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path
import java.util.*

/**
 * A Tiled Data Format (TDF) reader.
 *
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
                TdfTile.read(this)
            }
        }
    }

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

data class TdfDataset private constructor(
        val attributes: Map<String, String>,
        val dataType: TdfDataset.Type,
        val tileWidth: Int, val tileCount: Int,
        val tileOffsets: LongArray, val tileSizes: IntArray) {

    enum class Type {
        BYTE, SHORT, INT, FLOAT, DOUBLE, STRING
    }

    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            val attributes = readAttributes()
            val dataType = Type.valueOf(readCString())
            val tileWidth = readFloat().toInt()
            // ^^^ unsure why this is a float.

            val tileCount = readInt()
            val tileOffsets = LongArray(tileCount)
            val tileSizes = IntArray(tileCount)
            for (i in 0 until tileCount) {
                tileOffsets[i] = readLong()
                tileSizes[i] = readInt()
            }

            TdfDataset(attributes, dataType, tileWidth,
                       tileCount, tileOffsets, tileSizes)
        }
    }
}

interface TdfTile {
    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            val type = readCString()
            when (type) {
                "fixedStep" -> TODO()
                "variableStep" -> VariableTdfTile.read(this)
                "bed" -> BedTdfTile.read(this)
                "bedWithName" -> TODO()
                else -> error("unexpected type: $type")
            }
        }
    }
}

data class VariableTdfTile(val tileStart: Int,
                           val start: IntArray,
                           private val span: Int,
                           private val data: Array<FloatArray>) : TdfTile {
    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            val tileStart = readInt()
            val span = readFloat().toInt()  // Really?

            val size = readInt()
            val start = IntArray(size)
            for (i in 0 until size) {
                start[i] = readInt()
            }

            val sampleCount = readInt()
            val data = Array(sampleCount) {
                val acc = FloatArray(size)
                for (i in 0 until size) {
                    acc[i] = readFloat()
                }

                acc
            }

            VariableTdfTile(tileStart, start, span, data)
        }
    }
}

data class BedTdfTile(val start: IntArray,
                      val end: IntArray,
                      private val data: Array<FloatArray>) : TdfTile {
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

            val sampleCount = readInt()
            val data = Array(sampleCount) {
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