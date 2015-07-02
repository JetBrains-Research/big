package org.jbb.big

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import java.io.*
import java.nio.ByteOrder
import java.nio.channels.Channels
import java.nio.file.Path
import java.util.zip.Deflater
import java.util.zip.DeflaterOutputStream
import java.util.zip.Inflater
import java.util.zip.InflaterInputStream


/**
 * A stripped-down byte order-aware complement to [java.io.DataInputStream].
 */
public interface OrderedDataInput {
    /** Byte order used for compound data types. */
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
    public fun finished(): Boolean
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
    public inline fun with<T>(offset: Long, size: Long, compressed: Boolean,
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

    override fun finished() = tell() >= file.length()

    companion object {
        public fun of(path: Path,
                      order: ByteOrder = ByteOrder.nativeOrder()): SeekableDataInput {
            return SeekableDataInput(RandomAccessFile(path.toFile(), "r"), order)
        }
    }
}

public class ByteArrayDataInput(private val data: ByteArray,
                                public override val order: ByteOrder) : OrderedDataInput {
    private val input: DataInput = DataInputStream(ByteArrayInputStream(data))
    private var bytesRead: Int = 0

    override fun readUnsignedByte(): Int {
        check(!finished(), "no data")
        val b = input.readUnsignedByte()
        bytesRead++
        return b
    }

    override fun finished() = bytesRead >= data.size()
}

/**
 * A byte order-aware seekable complement to [java.io.DataOutputStream].
 */
public open class SeekableDataOutput(private val file: RandomAccessFile,
                                     public var order: ByteOrder) :
        DataOutput, Closeable, AutoCloseable {
    protected open val output: DataOutput get() = file

    public fun seek(pos: Long): Unit = file.seek(pos)

    public fun tell(): Long = file.getFilePointer()

    override fun write(b: ByteArray?) = output.write(b)

    override fun write(b: ByteArray?, off: Int, len: Int) = output.write(b, off, len)

    override fun writeBytes(s: String) = output.writeBytes(s)

    public fun writeBytes(s: String, length: Int) {
        file.writeBytes(s)
        writeByte(0, length - s.length())
    }

    override fun writeBoolean(v: Boolean) = output.writeBoolean(v)

    override fun writeByte(v: Int) = output.writeByte(v)

    public fun writeByte(v: Int, count: Int) {
        for (i in 0 until count) {
            writeByte(v)
        }
    }

    override fun write(b: Int) = output.write(b)

    override fun writeChar(v: Int) = output.writeChar(v)

    override fun writeShort(v: Int) {
        output.writeShort(if (order == ByteOrder.BIG_ENDIAN) {
            v
        } else {
            val b = Shorts.toByteArray(v.toShort())
            Shorts.fromBytes(b[1], b[0]).toInt()
        })
    }

    public fun writeUnsignedShort(v: Int) {
        output.writeShort(if (order == ByteOrder.BIG_ENDIAN) {
            v
        } else {
            val b = Ints.toByteArray(v)
            Shorts.fromBytes(b[3], b[2]).toInt()
        })
    }

    override fun writeInt(v: Int) {
        output.writeInt(if (order == ByteOrder.BIG_ENDIAN) {
            v
        } else {
            val b = Ints.toByteArray(v)
            Ints.fromBytes(b[3], b[2], b[1], b[0])
        })
    }

    override fun writeLong(v: Long): Unit {
        output.writeLong(if (order == ByteOrder.BIG_ENDIAN) {
            v
        } else {
            val b = Longs.toByteArray(v)
            Longs.fromBytes(b[7], b[6], b[5], b[4], b[3], b[2], b[1], b[0])
        })
    }

    override fun writeFloat(v: Float) = writeInt(java.lang.Float.floatToIntBits(v))

    override fun writeDouble(v: Double): Unit = writeLong(java.lang.Double.doubleToLongBits(v))

    override fun writeChars(s: String): Unit = throw UnsupportedOperationException()

    override fun writeUTF(s: String): Unit = throw UnsupportedOperationException()

    override fun close() = file.close()

    companion object {
        public fun of(path: Path, order: ByteOrder = ByteOrder.BIG_ENDIAN): SeekableDataOutput {
            return SeekableDataOutput(RandomAccessFile(path.toFile(), "rw"), order)
        }
    }
}