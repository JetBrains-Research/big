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
        bufferSize: Int = DEFAULT_BUFFER_SIZE,
        val doubleBuffer: Boolean = true
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

    // Actual buffer size is up to this value
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
        val cachedStart = bufferStartOffset
        val cachedEnd = bufferEndOffset

        var readBytes = 0
        var dstOffset = offset // last written offset in requested buffer
        if (requestEndOffset >= cachedEnd) {
            if (position in cachedStart until cachedEnd) {
                // requested buffer prefix intersection: copy prefix and proceed
                val inCBuffPos = (position - cachedStart).toInt()
                val count = (cachedEnd - cachedStart).toInt() - inCBuffPos
                System.arraycopy(this.buffer!!, inCBuffPos, buffer, dstOffset, count)
                readBytes = count
                dstOffset += count
                // move position
                seek(position + count)
            }
        } else if (requestEndOffset >= cachedStart) {
            // requested buffer suffix intersection: copy prefix and proceed
            val inCBuffPos = max(0, (position - cachedStart).toInt())
            val count = (requestEndOffset - cachedStart).toInt() - inCBuffPos
            System.arraycopy(this.buffer!!, inCBuffPos, buffer, dstOffset + length - count, count)
            readBytes = count
            // do not move position
        }

        while (readBytes < length) {
            fillBuffer()

            val newCachedEnd = bufferEndOffset
            if (newCachedEnd == -1L) {
                // eof
                break
            }
            val available = (newCachedEnd - position).toInt()
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
        val cachedStart = bufferStartOffset
        val cachedEnd = bufferEndOffset

        if (cachedEnd == -1L) {
            return -1 // eof
        }

        if (position !in cachedStart until cachedEnd) {
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
        if (!doubleBuffer) {
            return
        }
        // cache:         --------
        // opt1 :  *****
        // opt2 :      -----
        // opt3 :          -----
        // opt4 :               ------
        // opt5 :                    *****
        // opt6 :       ----------------
        val cachedStart = bufferStartOffset
        val cachedEnd = bufferEndOffset
        val maxBuffSize = buffer!!.size
        val newEnd = position + maxBuffSize

        if ((position < cachedStart && newEnd <= cachedStart) // new buffer before cache 
                || (position >= cachedEnd)) { // new buffer after cache

            // switch buffers:
            useSndBuffer = !useSndBuffer
        }
    }

    protected open fun fillBuffer(): Int {
        switchBuffersIfNeeded()

        val buff = buffer!!
        val maxBuffSize = buff.size

        val cachedStart = bufferStartOffset
        val cachedEnd = bufferEndOffset

        var actuallyReadBytes = 0
        val desiredNextEndOffset = position + maxBuffSize
        when {
            position >= cachedEnd -> {
                // cache:   -------
                // opt1 :            ??????????

                // doesn't intersect existing buffer
                actuallyReadBytes = fetchNewBuffer(position, 0, maxBuffSize)
            }

            position >= cachedStart -> {
                // cache:         -------
                // opt1 :           -----???????
                // opt2 :           ----

                // double buffer was switched to the other one containing data
                // do nothing
            }

            desiredNextEndOffset > cachedStart -> {
                // cache:         -------
                // opt1 :  ???????-----
                // opt2 :       ??-------???

                // copy intersecting part: Let's copy only left part and don't try to read small left
                // part. Better to read left part next time as a large buffer.
                val count = (minOf(desiredNextEndOffset, cachedEnd) - cachedStart).toInt()

                val remainingPrefixLength = (cachedStart - position).toInt()
                System.arraycopy(buff, 0, buff, remainingPrefixLength, count)

                // fetch remaining part at left side
                try {
                    actuallyReadBytes = fetchNewBuffer(position, 0, remainingPrefixLength)
                    check(bufferEndOffset != -1L) {
                        "Read backward, cannot be eof. Position $position, $count"
                    }

                    if (actuallyReadBytes == remainingPrefixLength) {
                        // forward already read part
                        bufferEndOffset += count
                    }
                    // else we have a gap => trim buffer and ignore shared count
                } catch (e: Exception) {
                    // we cannot left buffer in corrupted state, let's drop it.
                    // Also we have to break invariant that position is in buffer, seems
                    // it isn't an issue because we rethrow error
                    //
                    // alternative version is to copy common part in temporary buffer
                    // then fetch new buffer and finally copy temp buffer to ours buffer
                    // Seems this case is a rare on in general, so let's just drop buffer

                    // that position is in buffer after filling buffer, so we have not o
                    bufferStartOffset = 0
                    bufferEndOffset = 0
                    throw e
                }
            }
            else -> {
                // cache:               -------
                // opt1 :   ??????????

                // doesn't intersect existing buffer
                actuallyReadBytes = fetchNewBuffer(position, 0, maxBuffSize)
            }
        }
        return actuallyReadBytes

    }

    internal open fun fetchNewBuffer(pos: Long, buffOffset: Int, size: Int): Int {
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