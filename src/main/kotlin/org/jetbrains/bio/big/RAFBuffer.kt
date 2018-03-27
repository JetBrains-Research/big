package org.jetbrains.bio.big

import org.jetbrains.bio.RomBuffer
import ucar.unidata.io.RandomAccessFile
import java.nio.ByteOrder
import java.nio.file.Path

class RAFBufferFactory(private val path: Path, private val byteOrder: ByteOrder): RomBufferFactory {
    override fun create(): RomBuffer = RAFBuffer(path, byteOrder)

    override fun close() {
        // Do nothing
    }
}

class RAFBuffer(private val path: Path,
                override val order: ByteOrder,
                position: Long = 0L,
                limit: Long = -1) : RomBuffer() {

    private val randomAccessFile = RandomAccessFile(path.toAbsolutePath().toString(), "r", 128000).apply {
        order(order)
        seek(position)
    }

    private val maxLength =  randomAccessFile.length()
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

    override fun duplicate() = RAFBuffer(path, order, position, limit)

    override fun readInts(size: Int): IntArray {
        val dst = IntArray(size)
        randomAccessFile.readInt(dst,0, size)
        checkLimit()
        return dst
    }

    override fun readFloats(size: Int): FloatArray {
        val dst = FloatArray(size)
        randomAccessFile.readFloat(dst,0, size)
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