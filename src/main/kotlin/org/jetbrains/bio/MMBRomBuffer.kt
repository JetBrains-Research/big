package org.jetbrains.bio

import com.google.common.primitives.*
import com.indeed.util.mmap.MMapBuffer
import java.nio.ByteOrder

/** A read-only mapped buffer which supports files > 2GB .*/
open class MMBRomBuffer(
        private val mapped: MMapBuffer,
        override var position: Long = 0,
        limit: Long = mapped.memory().length()
) : RomBuffer() {

    override val order: ByteOrder get() = mapped.memory().order
    override var limit: Long = limit
        set(value) {
            val length = mapped.memory().length()
            check(value <= length) {
                "Limit $value is greater than buffer length $length"
            }
            field = value
        }

    /**
     * Returns a new buffer sharing the data with its parent.
     *
     * @see ByteBuffer.duplicate for details.
     */
    override fun duplicate(position: Long, limit: Long) = MMBRomBuffer(mapped, position, limit)

    override fun close() {
        /* Do nothing: BigWig file closes memory mapped buffer */
    }

    override fun readBytes(size: Int): ByteArray {
        val dst = ByteArray(size)
        mapped.memory().getBytes(position, dst)
        position += dst.size
        checkLimit()
        return dst
    }

    override fun readByte(): Byte {
        val value = mapped.memory().getByte(position)
        position += 1
        checkLimit()
        return value
    }

    override fun readShort(): Short {
        val value = mapped.memory().getShort(position)
        position += Shorts.BYTES
        checkLimit()
        return value
    }

    override fun readInts(size: Int): IntArray {
        val dst = IntArray(size)

        val buff = mapped.memory().intArray(position, size.toLong())
        buff.get(0, dst)
        position += size * Ints.BYTES
        checkLimit()
        return dst
    }

    override fun readInt(): Int {
        val value = mapped.memory().getInt(position)
        position += Ints.BYTES
        checkLimit()
        return value
    }

    override fun readLong(): Long {
        val value = mapped.memory().getLong(position)
        position += Longs.BYTES
        checkLimit()
        return value
    }

    override fun readFloats(size: Int): FloatArray {
        val dst = FloatArray(size)

        val value = mapped.memory().floatArray(position, size.toLong())!!
        value.get(0, dst)
        position += size * Floats.BYTES
        checkLimit()
        return dst
    }

    override fun readFloat(): Float {
        val value = mapped.memory().getFloat(position)
        position += Floats.BYTES
        checkLimit()
        return value
    }

    override fun readDouble(): Double {
        val value = mapped.memory().getDouble(position)
        position += Doubles.BYTES
        checkLimit()
        return value
    }
}