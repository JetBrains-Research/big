package org.jetbrains.bio

import com.google.common.primitives.*
import java.nio.ByteOrder

/**
 * Lightweight `RomBuffer` implementation which shares given `SeekableDataInput` with all its duplicates.
 * This buffer isn't thread safe if underlying `SeekableDataInput` also isn't thread safe. If you need
 * thread safe implementation please consider `org.jetbrains.bio.ThreadSafeStreamRomBuffer` class.
 *
 * NB: `ThreadSafeStreamRomBuffer` class relies on implementation details of this class to avoid
 * unnecessary synchronization. If you change this class estimate impact on `ThreadSafeStreamRomBuffer`
 * class please.
 */
open class LightweightRomBuffer(
        private val input: EndianSeekableDataInput,
        override val order: ByteOrder,
        private val maxLength: Long = input.length(),
        override var position: Long = 0,
        limit: Long = -1L
) : RomBuffer() {

    override var limit: Long = if (limit != -1L) limit else maxLength
        set(value) {
            check(value <= maxLength) {
                "Limit $value is greater than buffer length $maxLength"
            }
            field = value
        }

    /**
     * Returns a new buffer sharing the data with its parent.
     *
     * @see `java.nio.ByteBuffer#duplicate` for details.
     */
    override fun duplicate(position: Long, limit: Long) = LightweightRomBuffer(input, order, maxLength, position, limit)

    override fun close() {
        /* Do nothing: It is lightweight buffer */
    }

    override fun readBytes(size: Int): ByteArray {
        input.seek(position)
        input.order = order

        val dst = ByteArray(size)
        input.read(dst, 0, size)
        position += dst.size

        checkLimit()
        return dst
    }

    override fun readByte(): Byte {
        input.seek(position)
        input.order = order

        val value = input.readByte()
        position += 1

        checkLimit()
        return value
    }

    override fun readShort(): Short {
        input.seek(position)
        input.order = order

        val value = input.readShort()
        position += Shorts.BYTES

        checkLimit()
        return value
    }

    override fun readInts(size: Int): IntArray {
        input.seek(position)
        input.order = order

        val dst = IntArray(size)

        input.readInt(dst, 0, size)
        position += size * Ints.BYTES

        checkLimit()
        return dst
    }

    override fun readInt(): Int {
        input.seek(position)
        input.order = order

        val value = input.readInt()
        position += Ints.BYTES

        checkLimit()
        return value
    }

    override fun readLong(): Long {
        input.seek(position)
        input.order = order

        val value = input.readLong()
        position += Longs.BYTES
        checkLimit()
        return value
    }

    override fun readFloats(size: Int): FloatArray {
        input.seek(position)
        input.order = order

        val dst = FloatArray(size)

        input.readFloat(dst, 0, size)
        position += size * Floats.BYTES

        checkLimit()
        return dst
    }

    override fun readFloat(): Float {
        input.seek(position)
        input.order = order

        val value = input.readFloat()
        position += Floats.BYTES

        checkLimit()
        return value
    }

    override fun readDouble(): Double {
        input.seek(position)
        input.order = order

        val value = input.readDouble()
        position += Doubles.BYTES

        checkLimit()
        return value
    }
}