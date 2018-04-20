package org.jetbrains.bio

import java.nio.ByteOrder
import java.nio.file.Path

/**
 * @param path File path
 * @param byteOrder Byte order
 * @param bufferSize Random access file buffer size in bytes, use -1 for default value
 */
@Deprecated("Use HeavyweightRomBuffer or LightweightRomBuffer buffer")
open class RAFBuffer(
        private val path: Path,
        override val order: ByteOrder,
        position: Long = 0L,
        limit: Long = -1,
        val bufferSize: Int = -1,
        raf: RandomAccessFile = RandomAccessFile(path.toAbsolutePath().toString(), bufferSize)
) : RomBuffer() {

    private val randomAccessFile = raf.apply {
        order(order)
        seek(position)
    }

    private val maxLength = randomAccessFile.length()
    override var limit: Long = if (limit != -1L) limit else maxLength
        set(value) {
            val length = maxLength
            check(value <= length) {
                "Limit $value is greater than buffer length $length"
            }
            field = value
        }

    override var position: Long
        get() = randomAccessFile.filePointer
        set(position) { randomAccessFile.seek(position) }

    override fun duplicate(position: Long, limit: Long) = RAFBuffer(path, order, position, limit, bufferSize)

    override fun readInts(size: Int): IntArray {
        val dst = IntArray(size)
        randomAccessFile.readInt(dst, 0, size)
        checkLimit()
        return dst
    }

    override fun readFloats(size: Int): FloatArray {
        val dst = FloatArray(size)
        randomAccessFile.readFloat(dst, 0, size)
        checkLimit()
        return dst
    }

    override fun readBytes(size: Int): ByteArray {
        val dst = ByteArray(size)
        randomAccessFile.read(dst)
        checkLimit()
        return dst
    }

    override fun readByte(): Byte {
        val value = randomAccessFile.readByte()
        checkLimit()
        return value
    }

    override fun readShort(): Short {
        val value = randomAccessFile.readShort()
        checkLimit()
        return value
    }

    override fun readInt(): Int {
        val value = randomAccessFile.readInt()
        checkLimit()
        return value
    }

    override fun readLong(): Long {
        val value = randomAccessFile.readLong()
        checkLimit()
        return value
    }

    override fun readFloat(): Float {
        val value = randomAccessFile.readFloat()
        checkLimit()
        return value
    }

    override fun readDouble(): Double {
        val value = randomAccessFile.readDouble()
        checkLimit()
        return value
    }

    override fun close() {
        randomAccessFile.close()
    }
}