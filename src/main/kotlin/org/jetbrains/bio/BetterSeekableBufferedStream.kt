package org.jetbrains.bio

import htsjdk.samtools.seekablestream.SeekableStream
import kotlin.math.max
import kotlin.math.min

/**
 * @author Roman.Chernyatchik
 */
open class BetterSeekableBufferedStream(
        private val stream: SeekableStream,
        bufferSize: Int = DEFAULT_BUFFER_SIZE
) : SeekableStream() {

    var position: Long = 0
        protected set

    var bufferStartOffset: Long = 0 // inclusive
        protected set

    var bufferEndOffset: Long = 0 // exclusive
        protected set

    internal var buffer: ByteArray? = null
        get() {
            checkNotNull(field) { "Stream is closed" }
            return field
        }

    var bufferSize: Int
        get() = buffer!!.size
        set(bufferSize) {
            bufferStartOffset = 0
            bufferEndOffset = 0
            buffer = ByteArray(bufferSize)
        }

    init {
        this.bufferSize = if (bufferSize < 0) DEFAULT_BUFFER_SIZE else bufferSize
    }

    override fun length() = stream.length()

    override fun getSource() = stream.source

    override fun eof() = position >= length()

    override fun seek(position: Long) {
        require(position >= 0) { "Position should be non-negative value, but was $position" }

        // allow any position, even out of buffer bounds
        this.position = position
    }

    override fun position() = position

    override fun close() {
        buffer = null
        stream.close()
    }

    override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
        val cBuff = this.buffer!!

        val initialPos = position
        val requestEndOffset = initialPos + length

        var readBytes = 0
        var offset = 0 // last written offset in requested buffer
        if (requestEndOffset >= bufferEndOffset) {
            if (position in bufferStartOffset until bufferEndOffset) {
                // requested buffer prefix intersection: copy prefix and proceed
                val inCBuffPos = (position - bufferStartOffset).toInt()
                val count = (bufferEndOffset - bufferStartOffset).toInt() - inCBuffPos
                System.arraycopy(cBuff, inCBuffPos, buffer, 0, count)
                readBytes = count
                offset = count
                // move position
                seek(position + count)
            }
        } else if (requestEndOffset >= bufferStartOffset) {
            // requested buffer suffix intersection: copy prefix and proceed
            val inCBuffPos = max(0, (position - bufferStartOffset).toInt())
            val count = (requestEndOffset - bufferStartOffset).toInt() - inCBuffPos
            System.arraycopy(cBuff, inCBuffPos, buffer, length - count, count)
            readBytes = count
            // do not move position
        }

        while (readBytes < length) {
            fillBuffer()

            if (bufferEndOffset == -1L) {
                // eof
                break
            }
            val available = (bufferEndOffset - bufferStartOffset).toInt()
            val rest = length - readBytes
            val count = min(rest, available)
            System.arraycopy(cBuff, 0, buffer, offset, count)
            readBytes += count
            offset += count
            seek(position + count)
        }
        seek(initialPos + readBytes)

        return if (readBytes > 0) readBytes else -1
    }

    override fun read(): Int {
        if (bufferEndOffset == -1L) {
            return -1 // eof
        }

        if (position !in bufferStartOffset until bufferEndOffset) {
            fillBuffer()
            if (bufferEndOffset == -1L) {
                return -1 // eof
            }
        }

        val inBuffPos = (position - bufferStartOffset).toInt()
        position++
        return buffer!![inBuffPos].toInt() and 0xff
    }

    protected open fun fillBuffer() {
        val buff = buffer!!
        val buffSize = buff.size

        when {
            position >= bufferEndOffset -> {
                // doesn't intersect existing buffer
                fetchNewBuffer(position, 0, buffSize)
            }
            position >= bufferStartOffset -> {
                // copy intersecting part
                val relativePos = (position - bufferStartOffset).toInt()
                val available = (bufferEndOffset - bufferStartOffset).toInt()
                val count = available - relativePos
                System.arraycopy(buff, relativePos, buff, 0, count)

                // fetch remaining part
                fetchNewBuffer(bufferEndOffset, count, buffSize - count)
                if (count > 0) {
                    bufferStartOffset = position
                    if (bufferEndOffset == -1L) {
                        bufferEndOffset = bufferStartOffset + count
                    }
                }
            }
            position + buffSize > bufferStartOffset -> {
                // copy intersecting part
                val count = (position + buffSize - bufferStartOffset).toInt()
                System.arraycopy(buff, 0, buff, buffSize - count, count)

                // fetch remaining part
                fetchNewBuffer(position, 0, buffSize - count)
                check(bufferEndOffset != -1L) {
                    "Read backward, cannot be eof. Position $position, $count"
                }
                // forward already read part
                bufferEndOffset += count
            }
            else -> {
                // doesn't intersect existing buffer
                fetchNewBuffer(position, 0, buffSize)
            }
        }
    }

    protected open fun fetchNewBuffer(pos: Long, buffOffset: Int, size: Int) {
        stream.seek(pos)
        val buff = buffer!!

        val n = stream.read(buff, buffOffset, size)
        if (n <= 0) {
            bufferStartOffset = 0L
            bufferEndOffset = -1L
        } else {
            bufferStartOffset = pos
            bufferEndOffset = pos + n
        }
    }

    companion object {
        const val DEFAULT_BUFFER_SIZE = 128_000
    }
}