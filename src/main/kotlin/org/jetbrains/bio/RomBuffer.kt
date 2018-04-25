package org.jetbrains.bio

import com.google.common.primitives.Bytes
import org.iq80.snappy.Snappy
import org.jetbrains.bio.big.RTreeIndex
import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.DataFormatException
import java.util.zip.Inflater

/** A read-only buffer. */
abstract class RomBuffer: Closeable {
    abstract var position: Long
    abstract val order: ByteOrder
    abstract val maxLength: Long

    open var limit: Long = -1
        set(value) {
            check(value <= maxLength) {
                "Limit $value is greater than buffer length $maxLength"
            }
            field = if (value == -1L) maxLength else value
        }

    /**
     * Returns a new buffer sharing the data with its parent.
     *
     * @position:
     * @see ByteBuffer.duplicate for details.
     */
    abstract fun duplicate(position: Long, limit: Long): RomBuffer

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
        check(position <= limit) { "Buffer overflow: pos $position > limit $limit, max length: $maxLength" }
    }

    /**
     * Executes a `block` on a fixed-size possibly compressed input.
     *
     * This of this method as a way to get buffered input locally.
     * See for example [RTreeIndex.findOverlappingBlocks].
     */
    internal fun <T> with(offset: Long, size: Long,
                          compression: CompressionType = CompressionType.NO_COMPRESSION,
                          uncompressBufSize: Int,
                          block: RomBuffer.() -> T): T = decompress(
            offset, size, compression, uncompressBufSize
    ).use(block)

    internal fun decompress(offset: Long, size: Long,
                            compression: CompressionType = CompressionType.NO_COMPRESSION,
                            uncompressBufSize: Int): RomBuffer {

        return if (compression.absent) {
            duplicate(offset, offset + size)
        } else {
            val compressedBuf = duplicate(offset, limit).use {
                with(it) {
                    readBytes(com.google.common.primitives.Ints.checkedCast(size))
                }
            }

            var uncompressedSize: Int
            var uncompressedBuf: ByteArray
            when (compression) {
                CompressionType.DEFLATE -> {
                    uncompressedSize = 0
                    uncompressedBuf = ByteArray(when (uncompressBufSize) {
                        0 -> 2 * size.toInt()   // e.g in TDF
                        else -> uncompressBufSize
                    })

                    val inflater = inf
                    inflater.reset()
                    inflater.setInput(compressedBuf)
                    val sizeInt = size.toInt()
                    val step = sizeInt
                    var remaining = sizeInt
                    val maxUncompressedChunk = if (uncompressBufSize == 0) sizeInt else uncompressBufSize
                    try {
                        while (remaining > 0) {
                            uncompressedBuf = Bytes.ensureCapacity(
                                    uncompressedBuf,
                                    uncompressedSize + maxUncompressedChunk,
                                    maxUncompressedChunk / 2 // 1.5x
                            )

                            // start next chunk if smth remains
                            if (inflater.finished()) {
                                inflater.reset()
                                inflater.setInput(compressedBuf, sizeInt - remaining, remaining)
                            }

                            val actual = inflater.inflate(uncompressedBuf, uncompressedSize, maxUncompressedChunk)
                            remaining = inflater.remaining
                            uncompressedSize += actual
                        }
                    } catch (e: DataFormatException) {
                        val msg = """[@${Thread.currentThread().id}] java.util.zip.DataFormatException: ${e.message}
                            |  offset=$offset, limit=$limit, size=$size, compressed size=${compressedBuf.size}
                            |  remaining=$remaining, uncompressedSize=$uncompressedSize
                            |  uncompressBufSize=$uncompressBufSize
                            |""".trimMargin()
                        org.apache.log4j.Logger.getRootLogger().error(msg)
                        throw e
                    }
                    // Not obligatory, but let's left thread local variable in clean state
                    inflater.reset()
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

