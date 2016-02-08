package org.jetbrains.bio

import com.google.common.primitives.Bytes
import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
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

/** A read-only mapped buffer. */
class RomBuffer private constructor(val mapped: ByteBuffer) {
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
                          block: RomBuffer.() -> T): T {
        val input = if (compressed) {
            val compressedBuf = ByteArray(size.toInt())
            mapped.duplicate().apply {
                position(offset.toInt())
                get(compressedBuf)
            }

            inf.reset()
            inf.setInput(compressedBuf)
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
                position(offset.toInt())
                limit(Ints.checkedCast(offset + size))
            }
        }

        return with(RomBuffer(input.order(mapped.order())), block)
    }

    companion object {
        operator fun invoke(path: Path, order: ByteOrder = ByteOrder.nativeOrder()): RomBuffer {
            val mapped = FileChannel.open(path, StandardOpenOption.READ).use {
                it.map(MapMode.READ_ONLY, 0L, Files.size(path)).apply {
                    order(order)
                }
            }

            return RomBuffer(mapped)
        }
    }
}

/**
 * A stripped-down byte order-aware complement to [java.io.DataOutputStream].
 */
class OrderedDataOutput(private val output: OutputStream,
                        private val offset: Long,
                        val order: ByteOrder)
:
        Closeable, AutoCloseable {

    fun skipBytes(count: Int) {
        assert(count >= 0) { "count must be >=0" }
        output.write(ByteArray(count))
        ack(count)
    }

    fun writeByte(v: Int) {
        output.write(v)
        ack(1)
    }

    fun writeBoolean(v: Boolean) = writeByte(if (v) 1 else 0)

    fun writeShort(v: Int) {
        if (order == ByteOrder.BIG_ENDIAN) {
            output.write((v ushr 8) and 0xff)
            output.write((v ushr 0) and 0xff)
        } else {
            output.write((v ushr 0) and 0xff)
            output.write((v ushr 8) and 0xff)
        }
        
        ack(Shorts.BYTES)
    }

    fun writeInt(v: Int) {
        if (order == ByteOrder.BIG_ENDIAN) {
            output.write((v ushr 24) and 0xff)
            output.write((v ushr 16) and 0xff)
            output.write((v ushr  8) and 0xff)
            output.write((v ushr  0) and 0xff)
        } else {
            output.write((v ushr  0) and 0xff)
            output.write((v ushr  8) and 0xff)
            output.write((v ushr 16) and 0xff)
            output.write((v ushr 24) and 0xff)
        }

        ack(Ints.BYTES)
    }

    fun writeLong(v: Long) {
        if (order == ByteOrder.BIG_ENDIAN) {
            output.write((v ushr 56).toInt() and 0xff)
            output.write((v ushr 48).toInt() and 0xff)
            output.write((v ushr 40).toInt() and 0xff)
            output.write((v ushr 32).toInt() and 0xff)
            output.write((v ushr 24).toInt() and 0xff)
            output.write((v ushr 16).toInt() and 0xff)
            output.write((v ushr  8).toInt() and 0xff)
            output.write((v ushr  0).toInt() and 0xff)
        } else {
            output.write((v ushr  0).toInt() and 0xff)
            output.write((v ushr  8).toInt() and 0xff)
            output.write((v ushr 16).toInt() and 0xff)
            output.write((v ushr 24).toInt() and 0xff)
            output.write((v ushr 32).toInt() and 0xff)
            output.write((v ushr 40).toInt() and 0xff)
            output.write((v ushr 48).toInt() and 0xff)
            output.write((v ushr 56).toInt() and 0xff)
        }

        ack(Longs.BYTES)
    }

    fun writeFloat(v: Float) = writeInt(java.lang.Float.floatToIntBits(v))

    fun writeDouble(v: Double) = writeLong(java.lang.Double.doubleToLongBits(v))

    fun writeCString(s: String, length: Int = s.length + 1) {
        assert(s.length < length)
        output.write(s.toByteArray(Charsets.US_ASCII))
        val padding = length - s.length
        output.write(ByteArray(padding))
        ack(length)
    }

    /** Total number of bytes written. */
    private var written = 0L

    // This is important to keep lazy, otherwise the GC will be trashed
    // by a zillion of pending finalizers.
    private val def by ThreadLocal.withInitial { Deflater() }

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
            val inner = DeflaterOutputStream(output, def, 4096)
            OrderedDataOutput(inner, offset, order).block()
            inner.finish()
            ack(def.bytesWritten.toInt())
            def.bytesRead
        } else {
            val snapshot = written
            block()
            written - snapshot
        }.toInt()
    }

    fun tell() = offset + written

    override fun close() = output.close()

    companion object {
        fun of(path: Path, order: ByteOrder = ByteOrder.nativeOrder(),
               offset: Long = 0): OrderedDataOutput {
            val file = RandomAccessFile(path.toFile(), "rw")
            file.seek(offset)
            val output = Channels.newOutputStream(file.channel).buffered()
            return OrderedDataOutput(output, offset, order)
        }
    }
}
