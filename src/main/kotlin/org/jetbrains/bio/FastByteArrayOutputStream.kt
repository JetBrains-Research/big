package org.jetbrains.bio

import java.io.OutputStream
import java.util.*

class FastByteArrayOutputStream(private val capacity: Int = 8192)
:
        OutputStream() {

    private var pos = 0
    private var buf = ByteArray(capacity)
    private var completed = ArrayList<ByteArray>()

    private var closed = false

    override fun write(b: Int) {
        assert(!closed)
        tryComplete()
        buf[pos++] = b.toByte()
    }

    override fun write(src: ByteArray, offset: Int, length: Int) {
        assert(!closed)
        assert(offset > 0 && length > 0 && offset + length < src.size)

        var offset = offset
        var length = length
        while (length > 0) {
            tryComplete()
            val available = Math.min(length, capacity - pos)
            System.arraycopy(src, pos, buf, offset, available)
            offset += available
            pos += available
            length -= available
        }
    }

    fun toByteArray(): ByteArray {
        var size = buf.size
        for (other in completed) {
            size += other.size
        }

        return ByteArray(size).apply {
            var written = 0
            for (other in completed) {
                System.arraycopy(other, 0, this, written, other.size)
                written += other.size
            }

            System.arraycopy(buf, 0, this, written, pos)
        }
    }

    private fun tryComplete() {
        if (pos == capacity) {
            completed.add(buf)
            buf = ByteArray(capacity)
            pos = 0
        }
    }

    override fun close() {
        closed = true
    }
}
