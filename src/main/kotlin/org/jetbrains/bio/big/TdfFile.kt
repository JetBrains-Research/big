package org.jetbrains.bio.big

import com.google.common.primitives.Ints
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path

/**
 * A Tiled Data Format (TDF) reader.
 *
 * See https://www.broadinstitute.org/software/igv/TDF.
 */
class TdfFile @Throws(IOException::class) private constructor(path: Path) :
        Closeable, AutoCloseable {

    internal val input = SeekableDataInput.of(path)
    internal val header = Header.read(input, MAGIC)
    internal val windowFunctions = (0 until input.readInt())
            .map { WindowFunction.read(input) }
    internal val trackType = TrackType.read(input)
    internal val trackLine = input.readCString().trim()
    internal val trackNames = (0 until input.readInt())
            .map { input.readCString() }
    internal val build = input.readCString()
    internal val compressed = (input.readInt() and 0x1) != 0

    init {
        check(input.tell() == header.headerSize.toLong() + Header.BYTES)
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

data class WindowFunction(val id: String) {
    companion object {
        fun read(input: OrderedDataInput) = with(input) {
            WindowFunction(readCString())
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