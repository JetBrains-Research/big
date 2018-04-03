package org.jetbrains.bio

import com.google.common.primitives.Bytes
import org.iq80.snappy.Snappy
import org.jetbrains.bio.big.RTreeIndex
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.Inflater

/** A read-only buffer. */
abstract class RomBuffer: Closeable {
    abstract var position: Long
    abstract val order: ByteOrder

    abstract var limit: Long

    /**
     * Returns a new buffer sharing the data with its parent.
     *
     * @see ByteBuffer.duplicate for details.
     */
    abstract fun duplicate(): RomBuffer

    fun checkHeader(leMagic: Int) {
        val magic = readInt()
        check(magic == leMagic) {
            val bigMagic = java.lang.Integer.reverseBytes(leMagic)
            "Unexpected header magic: Actual $magic doesn't match expected LE=$leMagic (BE=$bigMagic)"
        }
    }

    abstract fun readInts(size: Int): IntArray
    abstract fun readFloats(size: Int): FloatArray

    abstract fun readBytes(size: Int): ByteArray
    abstract fun readByte(): Byte

    fun readUnsignedByte() = java.lang.Byte.toUnsignedInt(readByte())

    abstract fun readShort(): Short

    fun readUnsignedShort() = java.lang.Short.toUnsignedInt(readShort())

    abstract fun readInt(): Int

    abstract fun readLong(): Long

    abstract fun readFloat(): Float

    abstract fun readDouble(): Double

    fun readCString(): String {
        val sb = StringBuilder()
        do {
            val ch = readByte()
            if (ch == 0.toByte()) {
                break
            }

            sb.append(ch.toChar())
        } while (true)

        return sb.toString()
    }

    // Real buffer often is limited by current block size, so compare with 'limit', not eof or real input size
    fun hasRemaining() = position < limit

    protected fun checkLimit() {
        check(position <= limit) { "Buffer overflow: pos $position > limit $limit" }
    }


    /**
     * Executes a `block` on a fixed-size possibly compressed input.
     *
     * This of this method as a way to get buffered input locally.
     * See for example [RTreeIndex.findOverlappingBlocks].
     */
    internal fun <T> with(offset: Long, size: Long,
                          compression: CompressionType = CompressionType.NO_COMPRESSION,
                          block: RomBuffer.() -> T): T = decompress(offset, size, compression).use(block)

    internal fun decompress(offset: Long, size: Long,
                            compression: CompressionType = CompressionType.NO_COMPRESSION): RomBuffer {

        return if (compression.absent) {
            duplicate().apply {
                position = offset
                limit = offset + size
            }
        } else {
            val compressedBuf = duplicate().use {
                with(it) {
                    position = offset
                    readBytes(com.google.common.primitives.Ints.checkedCast(size))
                }
            }

            var uncompressedSize: Int
            var uncompressedBuf: ByteArray
            when (compression) {
                CompressionType.DEFLATE -> {
                    uncompressedSize = 0
                    uncompressedBuf = ByteArray(2 * size.toInt())

                    inf.reset()
                    inf.setInput(compressedBuf)
                    val step = size.toInt()
                    while (!inf.finished()) {
                        uncompressedBuf = Bytes.ensureCapacity(// 1.5x
                                uncompressedBuf, uncompressedSize + step, step / 2)
                        val actual = inf.inflate(uncompressedBuf, uncompressedSize, step)
                        uncompressedSize += actual
                    }
                    // Not obligatory, but let's left thread local variable in clean state
                    inf.reset()
                }
                CompressionType.SNAPPY -> {
                    uncompressedSize = Snappy.getUncompressedLength(compressedBuf, 0)
                    uncompressedBuf = ByteArray(uncompressedSize)
                    Snappy.uncompress(compressedBuf, 0, compressedBuf.size,
                                      uncompressedBuf, 0)
                }
                CompressionType.NO_COMPRESSION -> {
                    impossible()
                }
            }
            val input = ByteBuffer.wrap(uncompressedBuf, 0, uncompressedSize)
            BBRomBuffer(input.order(order))
        }
    }

    companion object {
        // This is important to keep lazy, otherwise the GC will be trashed
        // by a zillion of pending finalizers.
        private val inf by ThreadLocal.withInitial { Inflater() }
    }
}

