package org.jetbrains.bio

import java.nio.ByteOrder

/**
 * @author Roman.Chernyatchik
 *
 * Thread safe implementation, but not efficient, because it opens new stream on each buffer duplication
 */
open class HeavyweightRomBuffer(
        private val path: String,
        final override val order: ByteOrder,
        position: Long = 0L,
        limit: Long = -1L,
        private val bufferSize: Int = -1,
        private val inputFactory: (String, ByteOrder, Int) -> EndianSeekableDataInput
) : RomBuffer() {

    private val input: SeekableDataInput = inputFactory(path, order, bufferSize)
    override val maxLength: Long = input.length()

    override var position
        get() = input.position()
        set(position) {
            input.seek(position)
        }

    init {
        @Suppress("LeakingThis")
        this.position = position
        @Suppress("LeakingThis")
        this.limit = limit
    }


    override fun duplicate(position: Long, limit: Long) = HeavyweightRomBuffer(path, order, position, limit, bufferSize, inputFactory)

    override fun close() {
        input.close()
    }

    override fun readInts(size: Int): IntArray {
        val dst = IntArray(size)
        input.readInt(dst, 0, size)
        checkLimit()
        return dst
    }

    override fun readFloats(size: Int): FloatArray {
        val dst = FloatArray(size)
        input.readFloat(dst, 0, size)
        checkLimit()
        return dst
    }

    override fun readBytes(size: Int): ByteArray {
        val dst = ByteArray(size)
        input.read(dst, 0, size)
        checkLimit()
        return dst
    }

    override fun readByte(): Byte {
        val value = input.readByte()
        checkLimit()
        return value
    }

    override fun readShort(): Short {
        val value = input.readShort()
        checkLimit()
        return value
    }

    override fun readInt(): Int {
        val value = input.readInt()
        checkLimit()
        return value
    }

    override fun readLong(): Long {
        val value = input.readLong()
        checkLimit()
        return value
    }

    override fun readFloat(): Float {
        val value = input.readFloat()
        checkLimit()
        return value
    }

    override fun readDouble(): Double {
        val value = input.readDouble()
        checkLimit()
        return value
    }
}