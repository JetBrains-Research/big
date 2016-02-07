package org.jetbrains.bio

import com.google.common.primitives.Ints
import java.io.Closeable
import java.io.OutputStream
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.zip.Deflater
import java.util.zip.DeflaterOutputStream
import java.util.zip.Inflater
import kotlin.LazyThreadSafetyMode.NONE

/** Guess byte order from a given `magic`. */
fun ByteBuffer.guess(magic: Int): Boolean {
    order(ByteOrder.LITTLE_ENDIAN)
    val littleMagic = getInt()
    if (littleMagic != magic) {
        val bigMagic = java.lang.Integer.reverseBytes(littleMagic)
        if (bigMagic != magic) {
            return false
        }

        order(ByteOrder.BIG_ENDIAN)
    }

    return true
}

fun ByteBuffer.getUnsignedByte(): Int {
    return java.lang.Byte.toUnsignedInt(get())
}

fun ByteBuffer.getUnsignedShort(): Int {
    return java.lang.Short.toUnsignedInt(getShort())
}

fun ByteBuffer.getCString(): String {
    val sb = StringBuilder()
    do {
        val ch = get()
        if (ch == 0.toByte()) {
            break
        }

        sb.append(ch.toChar())
    } while (true)

    return sb.toString()
}

/**
 * A stripped-down byte order-aware complement to [java.io.DataInput].
 */
internal class SeekableDataInput private constructor(
        private val path: Path,
        order: ByteOrder)
:
        Closeable, AutoCloseable {

    private val channel = FileChannel.open(path, StandardOpenOption.READ)
    val mapped = channel.map(MapMode.READ_ONLY, 0L, Files.size(path)).apply {
        order(order)
    }

    // This is important to keep lazy, otherwise the GC will be trashed
    // by a zillion of pending finalizers.
    private val inf by lazy(NONE) { Inflater() }

    // For performance reasons we use fixed-size buffers for both
    // compressed and uncompressed inputs. Unfortunately this makes
    // the class non-thread safe.
    private var compressedBuf = ByteArray(1024)
    private var uncompressedBuf = ByteArray(4096)

    /**
     * Executes a `block` on a fixed-size possibly compressed input.
     *
     * This of this method as a way to get buffered input locally.
     * See for example [RTreeIndex.findOverlappingBlocks].
     */
    internal fun <T> with(offset: Long, size: Long,
                          compressed: Boolean = false,
                          block: ByteBuffer.() -> T): T {
        mapped.position(offset.toInt())
        val input = if (compressed) {
            compressedBuf = compressedBuf.ensureCapacity(size.toInt())
            mapped.get(compressedBuf, 0, size.toInt())

            // Decompression step is (unfortunately) mandatory, since
            // we need to know the *exact* length of the data before
            // passing it to `block`.
            inf.reset()
            inf.setInput(compressedBuf, 0, size.toInt())
            var uncompressedSize = 0
            var step = size.toInt()
            while (!inf.finished()) {
                uncompressedBuf = uncompressedBuf.ensureCapacity(uncompressedSize + step)
                val actual = inf.inflate(uncompressedBuf, uncompressedSize, step)
                uncompressedSize += actual
            }

            ByteBuffer.wrap(uncompressedBuf, 0, uncompressedSize)
        } else {
            mapped.duplicate().apply { limit(Ints.checkedCast(offset + size)) }
        }

        return with(input.order(mapped.order()), block)
    }

    override fun close() = channel.close()

    companion object {
        fun of(path: Path, order: ByteOrder = ByteOrder.nativeOrder()): SeekableDataInput {
            return SeekableDataInput(path, order)
        }
    }
}

private fun ByteArray.ensureCapacity(requested: Int): ByteArray {
    return if (size < requested) {
        copyOf((requested + requested shr 1).toInt())  // 1.5x
    } else {
        this
    }
}

/**
 * A stripped-down byte order-aware complement to [java.io.DataOutputStream].
 */
interface OrderedDataOutput {
    val order: ByteOrder

    fun skipBytes(count: Int) {
        assert(count >= 0) { "count must be >=0" }
        for (i in 0..count - 1) {
            writeByte(0)
        }
    }

    fun writeCString(s: String)

    fun writeCString(s: String, length: Int) {
        assert(length >= s.length + 1)
        writeCString(s)
        skipBytes(length - (s.length + 1))
    }

    fun writeBoolean(v: Boolean) = writeByte(if (v) 1 else 0)

    fun writeByte(v: Int)

    fun writeShort(v: Int) {
        if (order == ByteOrder.BIG_ENDIAN) {
            writeByte((v ushr 8) and 0xff)
            writeByte((v ushr 0) and 0xff)
        } else {
            writeByte((v ushr 0) and 0xff)
            writeByte((v ushr 8) and 0xff)
        }
    }

    fun writeInt(v: Int) {
        if (order == ByteOrder.BIG_ENDIAN) {
            writeByte((v ushr 24) and 0xff)
            writeByte((v ushr 16) and 0xff)
            writeByte((v ushr  8) and 0xff)
            writeByte((v ushr  0) and 0xff)
        } else {
            writeByte((v ushr  0) and 0xff)
            writeByte((v ushr  8) and 0xff)
            writeByte((v ushr 16) and 0xff)
            writeByte((v ushr 24) and 0xff)
        }
    }

    fun writeLong(v: Long) {
        if (order == ByteOrder.BIG_ENDIAN) {
            writeByte((v ushr 56).toInt() and 0xff)
            writeByte((v ushr 48).toInt() and 0xff)
            writeByte((v ushr 40).toInt() and 0xff)
            writeByte((v ushr 32).toInt() and 0xff)
            writeByte((v ushr 24).toInt() and 0xff)
            writeByte((v ushr 16).toInt() and 0xff)
            writeByte((v ushr  8).toInt() and 0xff)
            writeByte((v ushr  0).toInt() and 0xff)
        } else {
            writeByte((v ushr  0).toInt() and 0xff)
            writeByte((v ushr  8).toInt() and 0xff)
            writeByte((v ushr 16).toInt() and 0xff)
            writeByte((v ushr 24).toInt() and 0xff)
            writeByte((v ushr 32).toInt() and 0xff)
            writeByte((v ushr 40).toInt() and 0xff)
            writeByte((v ushr 48).toInt() and 0xff)
            writeByte((v ushr 56).toInt() and 0xff)
        }
    }

    fun writeFloat(v: Float) = writeInt(java.lang.Float.floatToIntBits(v))

    fun writeDouble(v: Double) = writeLong(java.lang.Double.doubleToLongBits(v))
}

internal open class CountingDataOutput(private val output: OutputStream,
                                       private val offset: Long,
                                       override val order: ByteOrder)
:
        OrderedDataOutput, Closeable, AutoCloseable {

    /** Total number of bytes written. */
    private var written = 0L

    // This is important to keep lazy, otherwise the GC will be trashed
    // by a zillion of pending finalizers.
    private val def by lazy(NONE) { Deflater() }

    private fun ack(size: Int) {
        written += size
    }

    /**
     * Executes a `block` (compressing the output) and returns the
     * total number of *uncompressed* bytes written.
     */
    fun with(compressed: Boolean, block: OrderedDataOutput.() -> Unit): Int {
        return if (compressed) {
            // This is slightly involved. We stack deflater on top of
            // our input stream and report the number of uncompressed
            // bytes fed into the deflater.
            def.reset()
            val inner = DeflaterOutputStream(output, def)
            with(CountingDataOutput(inner, offset, order), block)
            inner.finish()
            ack(def.bytesWritten.toInt())
            def.bytesRead
        } else {
            val snapshot = written
            with(this, block)
            written - snapshot
        }.toInt()
    }

    override fun writeCString(s: String) {
        for (ch in s) {
            output.write(ch.toInt())
        }

        output.write(0)  // null-terminated.
        ack(s.length + 1)
    }

    override fun writeByte(v: Int) {
        output.write(v)
        ack(1)
    }

    fun tell() = offset + written

    override fun close() = output.close()

    companion object {
        fun of(path: Path, order: ByteOrder = ByteOrder.nativeOrder(),
               offset: Long = 0): CountingDataOutput {
            val file = RandomAccessFile(path.toFile(), "rw")
            file.seek(offset)
            val output = Channels.newOutputStream(file.channel).buffered()
            return CountingDataOutput(output, offset, order)
        }
    }
}
