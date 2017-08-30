package org.jetbrains.bio

import com.google.common.primitives.*
import com.indeed.util.mmap.MMapBuffer
import org.iq80.snappy.Snappy
import org.jetbrains.bio.big.RTreeIndex
import java.io.Closeable
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.zip.Deflater
import java.util.zip.DeflaterOutputStream
import java.util.zip.Inflater

/** A read-only buffer. */
interface RomBuffer: Closeable, AutoCloseable {
    var position: Long
    val order: ByteOrder

    /**
     * Returns a new buffer sharing the data with its parent.
     *
     * @see ByteBuffer.duplicate for details.
     */
    fun duplicate(): RomBuffer

    fun checkHeader(leMagic: Int) {
        val magic = getInt()
        check(magic == leMagic) {
            val bigMagic = java.lang.Integer.reverseBytes(leMagic)
            "Unexpected header magic: Actual $magic doesn't match expected LE=$leMagic (BE=$bigMagic)"
        }
    }

    fun asIntArray(size: Int): IntArray
    fun asFloatArray(size: Int): FloatArray

    fun get(dst: ByteArray)
    fun get(): Byte

    fun getUnsignedByte() = java.lang.Byte.toUnsignedInt(get())

    fun getShort(): Short

    fun getUnsignedShort() = java.lang.Short.toUnsignedInt(getShort())

    fun getInt(): Int

    fun getLong(): Long

    fun getFloat(): Float

    fun getDouble(): Double

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

    fun hasRemaining(): Boolean
}

/** A read-only buffer based on [ByteBuffer]. */
class BBRomBuffer(private val buffer: ByteBuffer): RomBuffer {
    override var position: Long
        get() = buffer.position().toLong()
        set(value) = ignore(buffer.position(Ints.checkedCast(value)))

    override val order: ByteOrder get() = buffer.order()

    /**
     * Returns a new buffer sharing the data with its parent.
     *
     * @see ByteBuffer.duplicate for details.
     */
    override fun duplicate() = BBRomBuffer(buffer.duplicate().apply {
        order(buffer.order())
        position(Ints.checkedCast(position))
    })

    override fun close() { /* Do nothing */ }

    override fun asIntArray(size: Int) = IntArray(size).apply {
        buffer.asIntBuffer().get(this)
        buffer.position(buffer.position() + size * Ints.BYTES)
    }

    override fun asFloatArray(size: Int) = FloatArray(size).apply {
        buffer.asFloatBuffer().get(this)
        buffer.position(buffer.position() + size * Floats.BYTES)
    }

    override fun get(dst: ByteArray) { buffer.get(dst) }

    override fun get() = buffer.get()

    override fun getShort() = buffer.getShort()

    override fun getInt() = buffer.getInt()

    override fun getLong() = buffer.getLong()

    override fun getFloat() = buffer.getFloat()

    override fun getDouble() = buffer.getDouble()

    override fun hasRemaining() = buffer.hasRemaining()
}

/** A read-only mapped buffer which supports files > 2GB .*/
class MMBRomBuffer(val mapped: MMapBuffer,
                   override var position: Long = 0,
                   limit: Long = mapped.memory().length()) : RomBuffer {

    override val order: ByteOrder get() = mapped.memory().order
    private var limit: Long = limit
        set(value) {
            val length = mapped.memory().length()
            check(value <= length) {
                "Limit $value is greater than buffer length $length"
            }
            field = value
        }

    override fun hasRemaining() = position < limit

    /**
     * Returns a new buffer sharing the data with its parent.
     *
     * @see ByteBuffer.duplicate for details.
     */
    override fun duplicate(): MMBRomBuffer = MMBRomBuffer(mapped, position, limit)

    override fun close() { mapped.close() }

    private fun checkLimit() {
        check(position <= limit) { "Buffer overflow: pos $position > limit $limit" }
    }

    fun asIntBuffer(size: Long): com.indeed.util.mmap.IntArray {
        val value = mapped.memory().intArray(position, size)
        position += size * java.lang.Integer.BYTES
        checkLimit()
        return value
    }

    fun asFloatBuffer(size: Long): com.indeed.util.mmap.FloatArray {
        val value = mapped.memory().floatArray(position, size)!!
        position += size * java.lang.Float.BYTES
        checkLimit()
        return value

    }

    override fun asIntArray(size: Int): kotlin.IntArray {
        val dst = IntArray(size)
        asIntBuffer(size.toLong()).get(0, dst)
        checkLimit()
        return dst
    }

    override fun asFloatArray(size: Int): kotlin.FloatArray {
        val dst = FloatArray(size)
        asFloatBuffer(size.toLong()).get(0, dst)
        checkLimit()
        return dst
    }

    override fun get(dst: ByteArray) {
        mapped.memory().getBytes(position, dst)
        position += dst.size * java.lang.Byte.BYTES
        checkLimit()
    }

    override fun get(): Byte {
        val value = mapped.memory().getByte(position)
        position += java.lang.Byte.BYTES
        checkLimit()
        return value
    }

    override fun getShort(): Short {
        val value = mapped.memory().getShort(position)
        position += java.lang.Short.BYTES
        checkLimit()
        return value
    }

    override fun getInt(): Int {
        val value = mapped.memory().getInt(position)
        position += Integer.BYTES
        checkLimit()
        return value
    }

    override fun getLong(): Long {
        val value = mapped.memory().getLong(position)
        position += java.lang.Long.BYTES
        checkLimit()
        return value
    }

    override fun getFloat(): Float {
        val value = mapped.memory().getFloat(position)
        position += java.lang.Float.BYTES
        checkLimit()
        return value
    }

    override fun getDouble(): Double {
        val value = mapped.memory().getDouble(position)
        position += java.lang.Double.BYTES
        checkLimit()
        return value
    }

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
                          compression: CompressionType = CompressionType.NO_COMPRESSION,
                          block: RomBuffer.() -> T): T = decompress(offset, size, compression).block()

    internal fun decompress(offset: Long, size: Long,
                            compression: CompressionType = CompressionType.NO_COMPRESSION): RomBuffer {

        return if (compression.absent) {
            duplicate().apply {
                position = offset
                limit = offset + size
            }
        } else {
            val compressedBuf = ByteArray(size.toInt())
            duplicate().apply {
                position = offset
                get(compressedBuf)
            }

            var uncompressedSize: Int
            var uncompressedBuf: ByteArray
            when (compression) {
                CompressionType.DEFLATE -> {
                    uncompressedSize = 0
                    uncompressedBuf = ByteArray(2 * size.toInt())

                    inf.reset()
                    inf.setInput(compressedBuf)
                    val step = size.toInt()
                    while (!inf.finished()) {
                        uncompressedBuf = Bytes.ensureCapacity(// 1.5x
                                uncompressedBuf, uncompressedSize + step, step / 2)
                        val actual = inf.inflate(uncompressedBuf, uncompressedSize, step)
                        uncompressedSize += actual
                    }
                }
                CompressionType.SNAPPY -> {
                    uncompressedSize = Snappy.getUncompressedLength(compressedBuf, 0)
                    uncompressedBuf = ByteArray(uncompressedSize)
                    Snappy.uncompress(compressedBuf, 0, compressedBuf.size,
                                      uncompressedBuf, 0)
                }
                else -> impossible { "Unexpected compression: $compression" }
            }
            val input = ByteBuffer.wrap(uncompressedBuf, 0, uncompressedSize)
            BBRomBuffer(input.order(order))
        }
    }

    companion object {
        operator fun invoke(path: Path, order: ByteOrder)
                = MMBRomBuffer(MMapBuffer(path, MapMode.READ_ONLY, order))
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
                else -> impossible { "Unexpected compression: $compression" }
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
