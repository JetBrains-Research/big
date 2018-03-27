package org.jetbrains.bio

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import org.iq80.snappy.Snappy
import java.io.Closeable
import java.io.OutputStream
import java.nio.ByteOrder
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.zip.Deflater
import java.util.zip.DeflaterOutputStream

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

    fun writeString(s: String, length: Int = s.length) {
        assert(s.length <= length)
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
    fun with(compression: CompressionType, block: OrderedDataOutput.() -> Unit): Int {
        return if (compression.absent) {
            val snapshot = written
            block()
            (written - snapshot).toInt()
        } else {
            when (compression) {
                CompressionType.DEFLATE -> {
                    // This is slightly involved. We stack deflater on top of
                    // our input stream and report the number of uncompressed
                    // bytes fed into the deflater.
                    def.reset()
                    val inner = DeflaterOutputStream(output, def, 4096)
                    OrderedDataOutput(inner, offset, order).block()
                    inner.finish()
                    ack(def.bytesWritten.toInt())
                    def.bytesRead.toInt()
                }
                CompressionType.SNAPPY -> {
                    val inner = FastByteArrayOutputStream()
                    OrderedDataOutput(inner, offset, order).block()

                    val uncompressedBuf = inner.toByteArray()
                    val compressedBuf = ByteArray(Snappy.maxCompressedLength(uncompressedBuf.size))
                    val compressedSize = Snappy.compress(
                            uncompressedBuf, 0, uncompressedBuf.size,
                            compressedBuf, 0)
                    output.write(compressedBuf, 0, compressedSize)
                    ack(compressedSize)
                    uncompressedBuf.size
                }
                CompressionType.NO_COMPRESSION -> {
                    impossible()
                }
            }
        }
    }

    fun tell() = offset + written

    override fun close() = output.close()

    companion object {
        operator fun invoke(path: Path, order: ByteOrder = ByteOrder.nativeOrder(),
                            offset: Long = 0, create: Boolean = true): OrderedDataOutput {
            assert(offset == 0L || !create)
            val fc = if (create) {
                FileChannel.open(path,
                                 StandardOpenOption.CREATE,
                                 StandardOpenOption.TRUNCATE_EXISTING,
                                 StandardOpenOption.READ,
                                 StandardOpenOption.WRITE)
            } else {
                FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)
            }

            val output = Channels.newOutputStream(fc.position(offset)).buffered()
            return OrderedDataOutput(output, offset, order)
        }
    }
}