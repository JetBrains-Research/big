package org.jetbrains.bio

import htsjdk.samtools.seekablestream.SeekableStreamFactory
import java.nio.ByteOrder

/**
 * The factory creates buffer which isn't thread safe and couldn't be
 * used for concurrent `BigFile` access.
 */
open class EndianBufferFactory(private val stream: EndianSeekableDataInput) : RomBufferFactory {

    override var order: ByteOrder = stream.order
    private val maxLength = stream.length()

    override fun create(): RomBuffer = LightweightRomBuffer(stream, order, maxLength)

    override fun close() {
        stream.close()
    }

    companion object {
        /**
         * @param path File path or URL, see `htsjdk.samtools.seekablestream.SeekableStreamFactory#getStreamFor`
         * @param byteOrder Byte order: Little Endian or Big Endian, could be changed after stream has been created
         * @param bufferSize Buffer size, could be changed after stream has been created
         */
        fun create(path: String, byteOrder: ByteOrder,
                   bufferSize: Int = DEFAULT_BUFFER_SIZE) = EndianBufferFactory(
                EndianAwareDataSeekableStream(
                        BetterSeekableBufferedStream(
                                SeekableStreamFactory.getInstance().getStreamFor(path),
                                bufferSize
                        )).apply {
                    order = byteOrder
                }
        )
    }
}

/**
 * The factory creates thread safe buffer which could be used for concurrent `BigFile` access.
 */
open class EndianSynchronizedBufferFactory(private val stream: EndianSeekableDataInput) : RomBufferFactory {

    override var order: ByteOrder = stream.order
    private val maxLength = stream.length()

    override fun create(): RomBuffer = SynchronizedStreamAccessRomBuffer(stream, order, maxLength = maxLength)

    override fun close() {
        stream.close()
    }

    companion object {
        /**
         * @param path File path or URL, see `htsjdk.samtools.seekablestream.SeekableStreamFactory#getStreamFor`
         * @param byteOrder Byte order: Little Endian or Big Endian, could be changed after stream has been created
         * @param bufferSize Buffer size, could be changed after stream has been created
         */
        fun create(path: String, byteOrder: ByteOrder,
                   bufferSize: Int = DEFAULT_BUFFER_SIZE) = EndianSynchronizedBufferFactory(
                EndianAwareDataSeekableStream(
                        BetterSeekableBufferedStream(
                                SeekableStreamFactory.getInstance().getStreamFor(path),
                                bufferSize
                        )).apply {
                    order = byteOrder
                }
        )
    }
}

/**
 * The factory creates thread safe buffer which could be used for concurrent `BigFile` access.
 * Implementations isn't efficient due to large number of temporary stream open/close operations.
 * Buffer opens and closes stream for each buffer duplication, i.e for each data block. It works
 * ok if path points to file on hard disk, but will be extremely slow if file is http url.
 */
open class EndianThreadSafeBufferFactory(
        private val path: String,
        override var order: ByteOrder,
        private val bufferSize: Int = BetterSeekableBufferedStream.DEFAULT_BUFFER_SIZE)
    : RomBufferFactory {

    override fun create(): RomBuffer = HeavyweightRomBuffer(path, order,
                                                            bufferSize = bufferSize) { path, byteOrder, bufferSize ->
        EndianAwareDataSeekableStream(BetterSeekableBufferedStream(
                SeekableStreamFactory.getInstance().getStreamFor(path),
                bufferSize
        )).apply {
            order = byteOrder
        }
    }

    override fun close() {
        // Do nothing
    }
}
