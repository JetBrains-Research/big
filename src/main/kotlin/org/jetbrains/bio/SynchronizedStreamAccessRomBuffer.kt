package org.jetbrains.bio

import java.nio.ByteOrder

/**
 * Buffer implements thread safe in terms of underlying stream read/seek operations. Implementation isn't
 * thread safe in terms of concurrent class instance. Current architecture is used due to implementation
 * details of `big` library where concurrent operations results only in concurrent underlying big file
 * resource usage. E.g. we can keep opened BigWigFile for file provided by http stream. Concurrent
 * file access requires as to support concurrent http stream access. This implementation delegates
 * all i/o to underlying `LightweightRomBuffer` instance, which do seek before any read operation.
 * All i/o operations delegation is made in synchronized sections to ensure that seek and read operations
 * performed by `LightweightRomBuffer` for underlying `EndianSeekableDataInput` stream are consistent.
 */
class SynchronizedStreamAccessRomBuffer(
        private val input: EndianSeekableDataInput,
        override val order: ByteOrder,
        private val lock: Any = input,
        override val maxLength: Long = input.length(),
        position: Long = 0,
        limit: Long = -1L
) : RomBuffer() {

    // LightweightRomBuffer creation do not require synchronization for input stream
    private var buffer = LightweightRomBuffer(input, order, maxLength, position, limit)

    // synchronization for input stream not needed here
    override var position
        get() = buffer.position
        set(position) {
            buffer.position = position
        }

    // synchronization for input stream not needed here
    override var limit: Long
        get() = buffer.limit
        set(value) {
            buffer.limit = value
        }

    init {
        this.limit = limit
    }

    override fun duplicate(position: Long, limit: Long)
    // synchronization for input stream not needed here
            = SynchronizedStreamAccessRomBuffer(input, order, lock, maxLength, position, limit)

    override fun readInts(size: Int) = synchronized(lock) {
        buffer.readInts(size)
    }

    override fun readFloats(size: Int) = synchronized(lock) {
        buffer.readFloats(size)
    }

    override fun readBytes(size: Int) = synchronized(lock) {
        buffer.readBytes(size)
    }

    override fun readByte() = synchronized(lock) {
        buffer.readByte()
    }

    override fun readShort() = synchronized(lock) {
        buffer.readShort()
    }

    override fun readInt() = synchronized(lock) {
        buffer.readInt()
    }

    override fun readLong() = synchronized(lock) {
        buffer.readLong()
    }

    override fun readFloat() = synchronized(lock) {
        buffer.readFloat()
    }

    override fun readDouble() = synchronized(lock) {
        buffer.readDouble()
    }

    override fun readCString() = synchronized(lock) {
        doReadCString()
    }

    override fun close() {
        // synchronization for input stream not needed here
        buffer.close()
    }
}