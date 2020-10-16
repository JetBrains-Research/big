package org.jetbrains.bio

import htsjdk.samtools.seekablestream.ByteArraySeekableStream
import org.apache.commons.math3.util.Precision
import org.junit.Assert
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.io.EOFException
import java.nio.ByteOrder
import java.nio.ByteOrder.BIG_ENDIAN
import java.nio.ByteOrder.LITTLE_ENDIAN
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * @author Roman.Chernyatchik
 */
@RunWith(Parameterized::class)
class RomBufferTest(
        val fp: NamedRomBufferFactoryProvider,
        val byteOrder: ByteOrder
) {
    @Test
    fun defaults() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                assertEquals(0, rb1.position)
            }
        }
    }

    @Test
    fun duplicate() {
        fp("", byteOrder, limit = 10).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 2
                rb1.duplicate(rb1.position, rb1.limit).use { rbCopy ->
                    assertEquals(2, rbCopy.position)
                    assertEquals(10, rbCopy.limit)
                    assertEquals(rb1.order, rbCopy.order)

                    rbCopy.position = 8
                    assertEquals(8, rbCopy.position)
                }
                assertEquals(2, rb1.position)
            }
        }
    }

    @Test
    fun duplicate2() {
        fp("", byteOrder, limit = 10).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 2
                rb1.duplicate(4, 12).use { rbCopy ->
                    assertEquals(4, rbCopy.position)
                    assertEquals(12, rbCopy.limit)
                    assertEquals(rb1.order, rbCopy.order)

                    rbCopy.position = 8
                    assertEquals(8, rbCopy.position)
                }
                assertEquals(2, rb1.position)
            }
        }
    }

    @Test
    fun readByte() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 3
                assertEquals(7.toByte(), rb1.readByte())
                assertEquals(4, rb1.position)
            }
        }
    }

    @Test
    fun readUnsignedByte() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 3
                assertEquals(7.toByte(), rb1.readByte())
                assertEquals(4, rb1.position)
            }
        }
    }

    @Test
    fun readInts() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 4
                val expected = when (byteOrder) {
                    BIG_ENDIAN -> listOf(151027490, -8362907, 1718053110, -168496142)
                    else -> listOf(578748425, 1701085439, -160929946, -218893067)
                }
                assertEquals(expected, rb1.readInts(4).toList())
                assertEquals(20, rb1.position)
            }
        }
    }

    @Test
    fun readFloats() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 4
                val expected = when (byteOrder) {
                    BIG_ENDIAN -> floatArrayOf(1.5467217E-33f, Float.NaN, 2.7320071E23f, -6.210294E32f)
                    else -> floatArrayOf(3.4558963E-18f, 6.7442445E22f, -1.1784278E33f, -9.664127E30f)
                }
                assertArrayEquals(expected, rb1.readFloats(4), Precision.EPSILON.toFloat())
                assertEquals(20, rb1.position)
            }
        }
    }

    @Test
    fun readBytes() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 4
                assertEquals(listOf(9, 0, 127, 34), rb1.readBytes(4).map { it.toInt() })
                assertEquals(8, rb1.position)
            }
        }
    }

    @Test
    fun readShort() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 16
                assertEquals((if (byteOrder == BIG_ENDIAN) -2572 else -2827).toShort(),
                             rb1.readShort())
                assertEquals(18, rb1.position)
            }
        }
    }

    @Test
    fun readUnsignedShort() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 16
                assertEquals(if (byteOrder == BIG_ENDIAN) 62964 else 62709,
                             rb1.readUnsignedShort())
                assertEquals(18, rb1.position)
            }
        }
    }

    @Test
    fun readInt() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 16
                assertEquals(if (byteOrder == BIG_ENDIAN) -168496142 else -218893067,
                             rb1.readInt())
                assertEquals(20, rb1.position)
            }
        }
    }

    @Test
    fun readLong() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 16
                assertEquals(if (byteOrder == BIG_ENDIAN) -723685415333072914 else -1229499251294997259,
                             rb1.readLong())
                assertEquals(24, rb1.position)
            }
        }
    }

    @Test
    fun readFloat() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 16
                assertEquals(if (byteOrder == BIG_ENDIAN) -6.210294E32f else -9.664127E30f, rb1.readFloat(),
                             Precision.EPSILON.toFloat())
                assertEquals(20, rb1.position)
            }
        }
    }

    @Test
    fun readDouble() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 16
                assertEquals(if (byteOrder == BIG_ENDIAN) -1.6107974967240198E260 else -2.3646010511240337E226,
                             rb1.readDouble(),
                             Precision.EPSILON)
                assertEquals(24, rb1.position)
            }
        }
    }

    @Test
    fun readCString() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 1
                assertEquals(listOf(3, 4, 7, 9), rb1.readCString().toByteArray().map { it.toInt() })
                assertEquals(6, rb1.position)
            }
        }
    }

    @Test
    fun hasRemaining() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 1
                assertTrue(rb1.hasRemaining())

                rb1.position = (TEST_DATA.size - 1).toLong()
                assertTrue(rb1.hasRemaining())

                rb1.position++
                assertFalse(rb1.hasRemaining())

                // forward
                rb1.position = (TEST_DATA.size - 1).toLong()
                assertTrue(rb1.hasRemaining())
            }
        }
    }

    @Test
    fun hasRemainingLimited() {
        fp("", byteOrder, 10).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 1
                assertTrue(rb1.hasRemaining())

                rb1.position = 9
                assertTrue(rb1.hasRemaining())

                rb1.position = 10
                assertFalse(rb1.hasRemaining())

                // forward
                rb1.position = 9
                assertTrue(rb1.hasRemaining())
            }
        }
    }

    @Test
    fun hasRemainingLimitedToZero() {
        fp("", byteOrder, 0).use { factory ->
            factory.create().use { rb1 ->
                assertFalse(rb1.hasRemaining())
                rb1.position = 0
                assertFalse(rb1.hasRemaining())
            }
        }
    }

    @Test(expected = EOFException::class)
    fun checkLimit() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = TEST_DATA.size.toLong()
                rb1.readByte()
            }
        }
    }

    @Test
    fun checkLimitLimited() {
        val ex = Assert.assertThrows(IllegalStateException::class.java) {
            fp("", byteOrder, 10).use { factory ->
                factory.create().use { rb1 ->
                    rb1.position = 10
                    rb1.readByte()
                }
            }
        }
        assertEquals("Buffer overflow: pos 11 > limit 10, max length: 27", ex.message)
    }

    @Test
    fun checkLimitUnLimited() {
        fp("", byteOrder).use { factory ->
            factory.create().use { rb1 ->
                rb1.limit = -1
                rb1.position = 2
                assertEquals(4.toByte(), rb1.readByte())
            }
        }
    }

    @Test
    fun subbufferPosition() {
        fp("", byteOrder, 10).use { factory ->
            factory.create().use { rb1 ->
                rb1.position = 1
                assertEquals(1, rb1.position)
                assertEquals(3.toByte(), rb1.readByte())
                assertEquals(2, rb1.position)


                factory.create().use { rb2 ->
                    rb2.position = 5
                    assertEquals(5, rb2.position)
                    assertEquals(0.toByte(), rb2.readByte())
                    assertEquals(6, rb2.position)
                }
                assertEquals(2, rb1.position)
            }
        }
    }

    @Test
    fun checkHeaderCorrectOrder() {
        val leMagic = if (byteOrder == BIG_ENDIAN) BE_MAGIC else LE_MAGIC

        fp("", byteOrder, 10).use { factory ->
            factory.create().use { rb1 ->
                rb1.checkHeader(leMagic)
                assertTrue(true)
            }
        }
    }

    @Test
    fun checkHeaderReversedOrder() {
        val magic = if (byteOrder == BIG_ENDIAN) BE_MAGIC else LE_MAGIC
        val otherMagic = if (byteOrder == BIG_ENDIAN) LE_MAGIC else BE_MAGIC

        val ex = Assert.assertThrows(IllegalStateException::class.java) {
            fp("", byteOrder, 10).use { factory ->
                factory.create().use { rb1 ->
                    rb1.checkHeader(otherMagic)
                }
            }
        }
        assertEquals(
            "Unexpected header magic: Actual $magic doesn't match expected LE=$otherMagic (BE=$magic)",
            ex.message
        )
    }

    @Test
    fun checkHeaderUnknownMagic() {
        val magic = if (byteOrder == BIG_ENDIAN) BE_MAGIC else LE_MAGIC

        val ex = Assert.assertThrows(IllegalStateException::class.java) {
            fp("", byteOrder, 10).use { factory ->
                factory.create().use { rb1 ->
                    rb1.checkHeader(0)
                }
            }
        }
        assertEquals("Unexpected header magic: Actual $magic doesn't match expected LE=0 (BE=0)", ex.message)
    }

    companion object {
        val TEST_DATA = byteArrayOf(1, 3, 4, 7, 9, 0, Byte.MAX_VALUE, 34, -1, Byte.MIN_VALUE, 100,
                                    101, 102, 103, 104, -10, -11, -12, -13, -14, -15,
                                    -16, -17, -18, '\n'.toByte(), 11, 12)
        const val LE_MAGIC = 117703425 // 117703425
        const val BE_MAGIC = 16974855 // first int as BE

        fun createStream() = EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA))
        @Parameterized.Parameters(name = "{0}:{1}")
        @JvmStatic
        fun data(): Iterable<Array<Any>> {
            val romBufferFactoryProviders = listOf(
                    object : NamedRomBufferFactoryProvider("LightweightRomBuffer") {
                        override fun invoke(path: String, byteOrder: ByteOrder, limit: Long): RomBufferFactory {
                            val stream = createStream()

                            return object : RomBufferFactory {
                                override var order: ByteOrder = byteOrder
                                override fun create() = LightweightRomBuffer(stream, order, limit = limit)
                                override fun close() {
                                    stream.close()
                                }
                            }
                        }
                    },

                    object : NamedRomBufferFactoryProvider("HeavyweightRomBuffer") {
                        override fun invoke(path: String, byteOrder: ByteOrder, limit: Long): RomBufferFactory {
                            return object : RomBufferFactory {
                                override var order: ByteOrder = byteOrder
                                override fun create() = HeavyweightRomBuffer(path, order, limit = limit) { _, byteOrder, _ ->

                                    createStream().apply {
                                        order = byteOrder
                                    }
                                }

                                override fun close() { /* Do nothing */
                                }
                            }
                        }
                    },

                    object : NamedRomBufferFactoryProvider("ThreadSafeStreamRomBuffer") {
                        override fun invoke(path: String, byteOrder: ByteOrder, limit: Long): RomBufferFactory {
                            val stream = createStream()

                            return object : RomBufferFactory {
                                override var order: ByteOrder = byteOrder
                                override fun create() = SynchronizedStreamAccessRomBuffer(stream, order, limit = limit)
                                override fun close() {
                                    stream.close()
                                }
                            }
                        }
                    }
            )

            val params = mutableListOf<Array<Any>>()

            for (byteOrder in arrayOf(BIG_ENDIAN, LITTLE_ENDIAN)) {
                for (fp in romBufferFactoryProviders) {
                    params.add(arrayOf(fp, byteOrder))
                }
            }

            return params
        }
    }
}
