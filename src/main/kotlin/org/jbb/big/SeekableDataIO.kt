package org.jbb.big

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import java.io.*
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.zip.Deflater
import java.util.zip.Inflater

/**
 * A stripped-down byte order-aware complement to [java.io.DataInputStream].
 */
public interface OrderedDataInput {
    public val order: ByteOrder

    public fun readBoolean(): Boolean = readUnsignedByte() != 0

    public fun readByte(): Byte = readUnsignedByte().toByte()

    public fun readUnsignedByte(): Int

    public fun readShort(): Short {
        val b1 = readByte()
        val b2 = readByte()
        return if (order == ByteOrder.BIG_ENDIAN) {
            Shorts.fromBytes(b1, b2)
        } else {
            Shorts.fromBytes(b2, b1)
        }
    }

    public fun readUnsignedShort(): Int {
        val b1 = readByte()
        val b2 = readByte()
        return if (order == ByteOrder.BIG_ENDIAN) {
            Ints.fromBytes(0, 0, b1, b2)
        } else {
            Ints.fromBytes(0, 0, b2, b1)
        }
    }

    public fun readInt(): Int {
        val b1 = readByte()
        val b2 = readByte()
        val b3 = readByte()
        val b4 = readByte()
        return if (order == ByteOrder.BIG_ENDIAN) {
            Ints.fromBytes(b1, b2, b3, b4)
        } else {
            Ints.fromBytes(b4, b3, b2, b1)
        }
    }

    public fun readLong(): Long {
        val b1 = readByte()
        val b2 = readByte()
        val b3 = readByte()
        val b4 = readByte()
        val b5 = readByte()
        val b6 = readByte()
        val b7 = readByte()
        val b8 = readByte()
        return if (order == ByteOrder.BIG_ENDIAN) {
            Longs.fromBytes(b1, b2, b3, b4, b5, b6, b7, b8)
        } else {
            Longs.fromBytes(b8, b7, b6, b5, b4, b3, b2, b1)
        }
    }

    public fun readFloat(): Float = java.lang.Float.intBitsToFloat(readInt())

    public fun readDouble(): Double = java.lang.Double.longBitsToDouble(readLong())

    /**
     * Returns `true` if the input doesn't contain any more data and
     * `false` otherwise.
     * */
    public val finished: Boolean
}

public open class SeekableDataInput protected constructor(
        private val file: RandomAccessFile,
        public override var order: ByteOrder) : OrderedDataInput, Closeable, AutoCloseable {

    /** Guess byte order from a given big-endian `magic`. */
    public fun guess(magic: Int) {
        val b = ByteArray(4)
        readFully(b)
        val bigMagic = Ints.fromBytes(b[0], b[1], b[2], b[3])
        order = if (bigMagic != magic) {
            val littleMagic = Ints.fromBytes(b[3], b[2], b[1], b[0])
            check(littleMagic == magic, "bad signature")
            ByteOrder.LITTLE_ENDIAN
        } else {
            ByteOrder.BIG_ENDIAN
        }
    }

    /** Executes a `block` on a fixed-size possibly compressed input. */
    public fun with<T>(offset: Long, size: Long, compressed: Boolean,
                       block: OrderedDataInput.() -> T): T {
        seek(offset)
        val data = ByteArray(size.toInt())
        readFully(data)
        val input = ByteArrayDataInput(
                if (compressed) data.decompress() else data, order)
        return with(input, block)
    }

    public fun readFully(b: ByteArray, off: Int = 0, len: Int = b.size()) {
        file.readFully(b, off, len)
    }

    override fun readUnsignedByte(): Int = file.readUnsignedByte()

    public fun skipBytes(n: Int): Int = file.skipBytes(n)

    public open fun seek(pos: Long): Unit = file.seek(pos)

    public open fun tell(): Long = file.getFilePointer()

    override fun close() = file.close()

    override val finished: Boolean get() = tell() >= file.length()

    companion object {
        public fun of(path: Path,
                      order: ByteOrder = ByteOrder.nativeOrder()): SeekableDataInput {
            return SeekableDataInput(RandomAccessFile(path.toFile(), "r"), order)
        }
    }
}

private class ByteArrayDataInput(private val data: ByteArray,
                                 public override val order: ByteOrder) : OrderedDataInput {
    private val input: DataInput = DataInputStream(ByteArrayInputStream(data))
    private var bytesRead: Int = 0

    override fun readUnsignedByte(): Int {
        check(!finished, "no data")
        val b = input.readUnsignedByte()
        bytesRead++
        return b
    }

    override val finished: Boolean get() = bytesRead >= data.size()
}

/**
 * A stripped-down byte order-aware complement to [java.io.DataOutputStream].
 */
public interface OrderedDataOutput {
    public val order: ByteOrder

    fun skipBytes(v: Int, count: Int) {
        for (i in 0 until count) {
            writeByte(v)
        }
    }

    fun writeBytes(s: String)

    fun writeBytes(s: String, length: Int) {
        writeBytes(s)
        skipBytes(0, length - s.length())
    }

    fun writeBoolean(v: Boolean) = writeByte(if (v) 1 else 0)

    fun writeByte(v: Int)

    fun writeShort(v: Int) {
        val b = Shorts.toByteArray(v.toShort())
        if (order == ByteOrder.BIG_ENDIAN) {
            writeByte(b[0].toInt())
            writeByte(b[1].toInt())
        } else {
            writeByte(b[1].toInt())
            writeByte(b[0].toInt())
        }
    }

    public fun writeUnsignedShort(v: Int) {
        val b = Ints.toByteArray(v)
        if (order == ByteOrder.BIG_ENDIAN) {
            writeByte(b[2].toInt())
            writeByte(b[3].toInt())
        } else {
            writeByte(b[3].toInt())
            writeByte(b[2].toInt())
        }
    }

    fun writeInt(v: Int) {
        val b = Ints.toByteArray(v)
        if (order == ByteOrder.BIG_ENDIAN) {
            writeByte(b[0].toInt())
            writeByte(b[1].toInt())
            writeByte(b[2].toInt())
            writeByte(b[3].toInt())
        } else {
            writeByte(b[3].toInt())
            writeByte(b[2].toInt())
            writeByte(b[1].toInt())
            writeByte(b[0].toInt())
        }
    }

    fun writeLong(v: Long): Unit {
        val b = Longs.toByteArray(v)
        if (order == ByteOrder.BIG_ENDIAN) {
            writeByte(b[0].toInt())
            writeByte(b[1].toInt())
            writeByte(b[2].toInt())
            writeByte(b[3].toInt())
            writeByte(b[4].toInt())
            writeByte(b[5].toInt())
            writeByte(b[6].toInt())
            writeByte(b[7].toInt())
        } else {
            writeByte(b[7].toInt())
            writeByte(b[6].toInt())
            writeByte(b[5].toInt())
            writeByte(b[4].toInt())
            writeByte(b[3].toInt())
            writeByte(b[2].toInt())
            writeByte(b[1].toInt())
            writeByte(b[0].toInt())
        }
    }

    fun writeFloat(v: Float) = writeInt(java.lang.Float.floatToIntBits(v))

    fun writeDouble(v: Double): Unit = writeLong(java.lang.Double.doubleToLongBits(v))
}

public open class SeekableDataOutput(private val file: RandomAccessFile,
                                     public override var order: ByteOrder) :
        OrderedDataOutput, Closeable, AutoCloseable {

    /** Executes a `block` on a fixed-size possibly compressed input. */
    public fun with(compressed: Boolean, block: OrderedDataOutput.() -> Unit): Int {
        val offset = tell()
        val output = ByteArrayOutputStream()
        with(ByteArrayDataOutput(output, order), block)
        val data = output.toByteArray()
        file.write(if (compressed) data.compress() else data)
        return (tell() - offset).toInt()
    }

    override fun writeBytes(s: String) = file.writeBytes(s)

    override fun writeByte(v: Int) = file.writeByte(v)

    public fun seek(pos: Long): Unit = file.seek(pos)

    public fun tell(): Long = file.getFilePointer()

    override fun close() = file.close()

    companion object {
        public fun of(path: Path, order: ByteOrder = ByteOrder.BIG_ENDIAN): SeekableDataOutput {
            return SeekableDataOutput(RandomAccessFile(path.toFile(), "rw"), order)
        }
    }
}

private class ByteArrayDataOutput(private val data: ByteArrayOutputStream,
                                  public override val order: ByteOrder) : OrderedDataOutput {
    private val output: DataOutput = DataOutputStream(data)

    override fun writeBytes(s: String) = output.writeBytes(s)

    override fun writeByte(v: Int) = output.writeByte(v)
}

private fun ByteArray.decompress(): ByteArray {
    val inf = Inflater()
    inf.setInput(this)
    return ByteArrayOutputStream(size()).use { out ->
        val buf = ByteArray(1024)
        while (!inf.finished()) {
            val count = inf.inflate(buf)
            out.write(buf, 0, count)
        }

        out.toByteArray()
    }
}

private fun ByteArray.compress(): ByteArray {
    val def = Deflater()
    def.setInput(this)
    def.finish()
    return ByteArrayOutputStream(size()).use { out ->
        val buf = ByteArray(1024)
        while (!def.finished()) {
            val count = def.deflate(buf)
            out.write(buf, 0, count)
        }

        out.toByteArray()
    }
}