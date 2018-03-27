package org.jetbrains.bio

import com.google.common.primitives.Floats
import com.google.common.primitives.Ints
import java.nio.ByteBuffer
import java.nio.ByteOrder

/** A read-only buffer based on [ByteBuffer]. */
class BBRomBuffer internal constructor(private val buffer: ByteBuffer): RomBuffer() {
    override var position: Long
        get() = buffer.position().toLong()
        set(value) = ignore(buffer.position(Ints.checkedCast(value)))

    override var limit: Long
        get() = buffer.limit().toLong()
        set(value) { buffer.limit(Ints.checkedCast(value))}

    override val order: ByteOrder get() = buffer.order()

    /**
     * Returns a new buffer sharing the data with its parent.
     *
     * @see ByteBuffer.duplicate for details.
     */
    override fun duplicate() = BBRomBuffer(buffer.duplicate().apply {
        order(buffer.order())
        position(Ints.checkedCast(position))
    })

    override fun close() { /* Do nothing */ }

    override fun readInts(size: Int) = IntArray(size).apply {
        buffer.asIntBuffer().get(this)
        buffer.position(buffer.position() + size * Ints.BYTES)
    }

    override fun readFloats(size: Int) = FloatArray(size).apply {
        buffer.asFloatBuffer().get(this)
        buffer.position(buffer.position() + size * Floats.BYTES)
    }

    override fun readBytes(size: Int) = ByteArray(size).apply {
        buffer.get(this)
    }

    override fun readByte() = buffer.get()

    override fun readShort() = buffer.short

    override fun readInt() = buffer.int

    override fun readLong() = buffer.long

    override fun readFloat() = buffer.float

    override fun readDouble() = buffer.double
}