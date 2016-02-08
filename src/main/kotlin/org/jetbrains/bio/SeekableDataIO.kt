package org.jetbrains.bio

import com.google.common.primitives.Bytes
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

/** A [MappedByteBuffer] extended to handle compressed blocks. */
class BigByteBuffer private constructor(val mapped: ByteBuffer) {
    var position: Int
        get() = mapped.position()
        set(value: Int) = ignore(mapped.position(value))

    val order: ByteOrder get() = mapped.order()

    /** Guess byte order from a given `magic`. */
    fun guess(magic: Int): Boolean {
        mapped.order(ByteOrder.LITTLE_ENDIAN)
        val littleMagic = getInt()
        if (littleMagic != magic) {
            val bigMagic = java.lang.Integer.reverseBytes(littleMagic)
            if (bigMagic != magic) {
                return false
            }

            mapped.order(ByteOrder.BIG_ENDIAN)
        }

        return true
    }

    fun asIntBuffer() = mapped.asIntBuffer()

    fun asFloatBuffer() = mapped.asFloatBuffer()

    fun get(dst: ByteArray) = mapped.get(dst)

    fun get() = mapped.get()

    fun getUnsignedByte() = java.lang.Byte.toUnsignedInt(get())

    fun getShort() = mapped.getShort()

    fun getUnsignedShort() = java.lang.Short.toUnsignedInt(getShort())

    fun getInt() = mapped.getInt()

    fun getLong() = mapped.getLong()

    fun getFloat() = mapped.getFloat()

    fun getDouble() = mapped.getDouble()

    fun getCString(): String {
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

    fun hasRemaining() = mapped.hasRemaining()

    // This is important to keep lazy, otherwise the GC will be trashed
    // by a zillion of pending finalizers.
    private val inf by ThreadLocal.withInitial { Inflater() }

    /**
     * Executes a `block` on a fixed-size possibly compressed input.
     *
     * This of this method as a way to get buffered input locally.
     * See for example [RTreeIndex.findOverlappingBlocks].
     */
    internal fun <T> with(offset: Long, size: Long,
                          compressed: Boolean = false,
                          block: BigByteBuffer.() -> T): T {
        mapped.position(offset.toInt())
        val input = if (compressed) {
            val compressedBuf = ByteArray(size.toInt())
            mapped.get(compressedBuf, 0, size.toInt())

            inf.reset()
            inf.setInput(compressedBuf, 0, size.toInt())
            var step = size.toInt()
            var uncompressedSize = 0
            var uncompressedBuf = ByteArray(2 * step)
            while (!inf.finished()) {
                uncompressedBuf = Bytes.ensureCapacity(  // 1.5x
                        uncompressedBuf, uncompressedSize + step, step / 2)
                val actual = inf.inflate(uncompressedBuf, uncompressedSize, step)
                uncompressedSize += actual
            }

            ByteBuffer.wrap(uncompressedBuf, 0, uncompressedSize)
        } else {
            mapped.duplicate().apply {
                limit(Ints.checkedCast(offset + size))
            }
        }

        return with(BigByteBuffer(input.order(mapped.order())), block)
    }

    companion object {
        fun of(path: Path, order: ByteOrder = ByteOrder.nativeOrder()): BigByteBuffer {
            val mapped = FileChannel.open(path, StandardOpenOption.READ).use {
                it.map(MapMode.READ_ONLY, 0L, Files.size(path)).apply {
                    order(order)
                }
            }

            return BigByteBuffer(mapped)
        }
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
