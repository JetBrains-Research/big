package org.jetbrains.bio

import com.google.common.primitives.Ints
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

    private var useSndBuffer = false
    internal fun curBufIdx() = if (useSndBuffer) 1 else 0

    internal var bufferStartOffsets = arrayOf(0L, 0L)
    var bufferStartOffset: Long // inclusive
        get() = bufferStartOffsets[curBufIdx()]
        protected set(value) {
            bufferStartOffsets[curBufIdx()] = value
        }

    internal var bufferEndOffsets = arrayOf(0L, 0L)
    var bufferEndOffset: Long // exclusive
        get() = bufferEndOffsets[curBufIdx()]
        protected set(value) {
            bufferEndOffsets[curBufIdx()] = value
        }

    internal var buffers: Array<ByteArray?> = arrayOf(null, null)
    internal var buffer: ByteArray?
        get() {
            val buf = buffers[curBufIdx()]
            checkNotNull(buf) { "Stream is closed" }
            return buf
        }
        set(value) {
            buffers[curBufIdx()] = value
        }

    var bufferSize: Int
        get() = buffer!!.size
        set(bufferSize) {
            useSndBuffer = false
            buffers = arrayOf(ByteArray(bufferSize), ByteArray(bufferSize))
            bufferStartOffsets = arrayOf(0L, 0L)
            bufferEndOffsets = arrayOf(0L, 0L)
        }

    init {
        this.bufferSize = if (bufferSize < 0) DEFAULT_BUFFER_SIZE else bufferSize
    }

    override fun length() = stream.length()

    override fun getSource(): String? = stream.source

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
        val initialPos = position
        val requestEndOffset = initialPos + length

        var readBytes = 0
        var dstOffset = offset // last written offset in requested buffer
        if (requestEndOffset >= bufferEndOffset) {
            if (position in bufferStartOffset until bufferEndOffset) {
                // requested buffer prefix intersection: copy prefix and proceed
                val inCBuffPos = (position - bufferStartOffset).toInt()
                val count = (bufferEndOffset - bufferStartOffset).toInt() - inCBuffPos
                System.arraycopy(this.buffer!!, inCBuffPos, buffer, dstOffset, count)
                readBytes = count
                dstOffset += count
                // move position
                seek(position + count)
            }
        } else if (requestEndOffset >= bufferStartOffset) {
            // requested buffer suffix intersection: copy prefix and proceed
            val inCBuffPos = max(0, (position - bufferStartOffset).toInt())
            val count = (requestEndOffset - bufferStartOffset).toInt() - inCBuffPos
            System.arraycopy(this.buffer!!, inCBuffPos, buffer, dstOffset + length - count, count)
            readBytes = count
            // do not move position
        }

        while (readBytes < length) {
            fillBuffer()

            if (bufferEndOffset == -1L) {
                // eof
                break
            }
            val available = (bufferEndOffset - position).toInt()
            val remaining = length - readBytes
            val count = min(remaining, available)
            val inBuffPos = Ints.checkedCast(position - bufferStartOffset)
            System.arraycopy(this.buffer!!, inBuffPos, buffer, dstOffset, count)
            readBytes += count
            dstOffset += count
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

    internal fun switchBuffersIfNeeded() {
        val buffSize = buffer!!.size
        val curBuffRange = bufferStartOffset until bufferEndOffset
        // if current buffer intersects next one - do nothing
        // action required if next buffer is not intersecting current
        // otherwise impl will be too complicated
        if (position in curBuffRange || (position + buffSize) in curBuffRange) {
            // do nothing
            return
        }

        // switch buffers:
        useSndBuffer = !useSndBuffer
    }

    protected open fun fillBuffer() {
        switchBuffersIfNeeded()

        val buff = buffer!!
        val buffSize = buff.size

        when {
            position >= bufferEndOffset -> {
                // doesn't intersect existing buffer
                fetchNewBuffer(position, 0, buffSize)
            }
            position >= bufferStartOffset -> {
                // double buffer was switched to the other one containing data
                // do nothing
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

    protected open fun fetchNewBuffer(pos: Long, buffOffset: Int, size: Int): Int {
        stream.seek(pos)

        if (size == 0) {
            bufferStartOffset = pos
            bufferEndOffset = pos
            return 0
        }

        val buff = buffer!!

        val n = stream.read(buff, buffOffset, size)
        if (n <= 0) {
            bufferStartOffset = 0L
            bufferEndOffset = -1L
        } else {
            bufferStartOffset = pos
            bufferEndOffset = pos + n
        }
        return n
    }

    companion object {
        const val DEFAULT_BUFFER_SIZE = 128_000
    }
}