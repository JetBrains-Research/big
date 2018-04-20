package org.jetbrains.bio

import com.google.common.io.LittleEndianDataInputStream
import htsjdk.samtools.seekablestream.SeekableStream
import java.io.DataInputStream
import java.nio.ByteOrder

class EndianAwareDataSeekableStream(private val stream: SeekableStream) : EndianSeekableDataInput {
    override var order: ByteOrder = ByteOrder.BIG_ENDIAN

    private val beInput = DataInputStream(stream)
    private val leInput = LittleEndianDataInputStream(stream)

    override fun position() = stream.position()
    override fun length() = stream.length()

    override fun seek(position: Long) {
        stream.seek(position)
    }

    override fun close() {
        stream.close()
    }

    override fun read(buffer: ByteArray, offset: Int, length: Int) = stream.read(buffer, offset, length)

    override fun readInt(): Int = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readInt()
        else -> leInput.readInt()
    }

    override fun readLong() = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readLong()
        else -> leInput.readLong()
    }

    override fun readFloat() = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readFloat()
        else -> leInput.readFloat()
    }

    override fun readDouble() = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readDouble()
        else -> leInput.readDouble()
    }

    override fun readFully(b: ByteArray) {
        when (order) {
            ByteOrder.BIG_ENDIAN -> beInput.readFully(b)
            else -> leInput.readFully(b)
        }
    }

    override fun readFully(b: ByteArray, off: Int, len: Int) {
        when (order) {
            ByteOrder.BIG_ENDIAN -> beInput.readFully(b, off, len)
            else -> leInput.readFully(b, off, len)
        }
    }

    override fun readUnsignedShort() = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readUnsignedShort()
        else -> leInput.readUnsignedShort()
    }

    override fun readUnsignedByte() = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readUnsignedByte()
        else -> leInput.readUnsignedByte()
    }

    override fun readUTF() = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readUTF()
        else -> leInput.readUTF()
    }

    override fun readChar() = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readChar()
        else -> leInput.readChar()
    }

    @Suppress("DeprecatedCallableAddReplaceWith")
    @Deprecated("Unsupported for LE and doesn't work properly for BE stream. So let's always throw `UnsupportedOperationException`")
    @Throws(UnsupportedOperationException::class)
    override fun readLine(): String? {
        throw UnsupportedOperationException("readLine is not supported")
    }

    override fun readByte() = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readByte()
        else -> leInput.readByte()
    }

    override fun skipBytes(n: Int) = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.skipBytes(n)
        else -> leInput.skipBytes(n)
    }


    override fun readBoolean() = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readBoolean()
        else -> leInput.readBoolean()
    }


    override fun readShort() = when (order) {
        ByteOrder.BIG_ENDIAN -> beInput.readShort()
        else -> leInput.readShort()
    }
}
