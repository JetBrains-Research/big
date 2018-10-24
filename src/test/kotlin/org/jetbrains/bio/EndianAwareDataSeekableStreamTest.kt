package org.jetbrains.bio

import com.google.common.primitives.Chars
import htsjdk.samtools.seekablestream.ByteArraySeekableStream
import org.apache.commons.math3.util.Precision
import org.junit.Assert
import org.junit.Test
import java.nio.ByteOrder
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * @author Roman.Chernyatchik
 */
class EndianAwareDataSeekableStreamTest {
    @Test
    fun defaults() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use {
            assertEquals(ByteOrder.BIG_ENDIAN, it.order)
        }
    }

    @Test
    fun order() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            assertEquals(ByteOrder.BIG_ENDIAN, stream.order)
            stream.order = ByteOrder.LITTLE_ENDIAN
            assertEquals(ByteOrder.LITTLE_ENDIAN, stream.order)
        }
    }

    @Test
    fun length() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            assertEquals(TEST_DATA.size, stream.length().toInt())
        }
    }

    @Test
    fun seek() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.seek(3)
            assertEquals(7.toByte(), stream.readByte())
        }
    }

    @Test(expected = NullPointerException::class)
    fun close() {
        val stream = EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA))
        stream.close()
        stream.readByte()
    }

    @Test
    fun read() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            val buffer = ByteArray(10)

            stream.seek(3)
            stream.order = ByteOrder.BIG_ENDIAN
            stream.read(buffer, 2, 6)
            assertEquals(9, stream.position())
            assertEquals(TEST_DATA.toList().subList(3, 7),
                         buffer.toList().subList(2, 6))

            stream.seek(3)
            stream.order = ByteOrder.LITTLE_ENDIAN
            stream.read(buffer, 2, 6)
            assertEquals(9, stream.position())
            assertEquals(TEST_DATA.toList().subList(3, 7),
                         buffer.toList().subList(2, 6))
        }
    }

    @Test
    fun readInt() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            assertEquals(16974855, stream.readInt())
            assertEquals(4, stream.position())
            assertEquals(151027490, stream.readInt())

            stream.seek(0)
            stream.order = ByteOrder.LITTLE_ENDIAN
            assertEquals(117703425, stream.readInt())
            assertEquals(4, stream.position())
            assertEquals(578748425, stream.readInt())
        }
    }

    @Test
    fun readLong() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            assertEquals(72906447230369570, stream.readLong())
            assertEquals(8, stream.position())
            assertEquals(-35918410346436362, stream.readLong())

            stream.seek(0)
            stream.order = ByteOrder.LITTLE_ENDIAN
            assertEquals(2485705558104212225, stream.readLong())
            assertEquals(8, stream.position())
            assertEquals(-691188853315960577, stream.readLong())
        }

    }

    @Test
    fun readFloat() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            Assert.assertEquals(2.406379E-38f, stream.readFloat(), Precision.EPSILON.toFloat())
            assertEquals(4, stream.position())
            Assert.assertEquals(1.5467217E-33f, stream.readFloat(), Precision.EPSILON.toFloat())

            stream.seek(0)
            stream.order = ByteOrder.LITTLE_ENDIAN
            Assert.assertEquals(9.931459E-35f, stream.readFloat(), Precision.EPSILON.toFloat())
            assertEquals(4, stream.position())
            Assert.assertEquals(3.4558963E-18f, stream.readFloat(), Precision.EPSILON.toFloat())
        }
    }

    @Test
    fun readDouble() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            Assert.assertEquals(8.665376552567706E-304, stream.readDouble(), Precision.EPSILON)
            assertEquals(8, stream.position())
            Assert.assertEquals(-1.4388718507777476E306, stream.readDouble(), Precision.EPSILON)

            stream.seek(0)
            stream.order = ByteOrder.LITTLE_ENDIAN
            Assert.assertEquals(1.5888602043431183E-142, stream.readDouble(), Precision.EPSILON)
            assertEquals(8, stream.position())
            Assert.assertEquals(-2.401405978668564E262, stream.readDouble(), Precision.EPSILON)
        }

    }

    @Test
    fun readFully() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            val buffer = ByteArray(10)

            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            stream.readFully(buffer)
            assertEquals(TEST_DATA.toList().subList(0, 10), buffer.toList())
            assertEquals(10, stream.position())

            stream.order = ByteOrder.LITTLE_ENDIAN
            stream.seek(0)
            stream.readFully(buffer)
            assertEquals(TEST_DATA.toList().subList(0, 10), buffer.toList())
            assertEquals(10, stream.position())
        }
    }

    @Test
    fun readFully2() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            val buffer = ByteArray(20)

            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            stream.readFully(buffer, 2, 10)
            assertEquals(TEST_DATA.toList().subList(0, 10), buffer.toList().subList(2, 12))
            assertEquals(10, stream.position())

            stream.order = ByteOrder.LITTLE_ENDIAN
            stream.seek(0)
            stream.readFully(buffer, 2, 10)
            assertEquals(TEST_DATA.toList().subList(0, 10), buffer.toList().subList(2, 12))
            assertEquals(10, stream.position())
        }
    }

    @Test
    fun readUnsignedShort() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            assertEquals(259, stream.readUnsignedShort())
            assertEquals(2, stream.position())
            assertEquals(1031, stream.readUnsignedShort())
            stream.seek(20)
            assertEquals(61936, stream.readUnsignedShort())

            stream.seek(0)
            stream.order = ByteOrder.LITTLE_ENDIAN
            assertEquals(769, stream.readUnsignedShort())
            assertEquals(2, stream.position())
            assertEquals(1796, stream.readUnsignedShort())
            stream.seek(20)
            assertEquals(61681, stream.readUnsignedShort())
        }

    }

    @Test
    fun readUnsignedByte() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            assertEquals(1, stream.readUnsignedByte())
            assertEquals(1, stream.position())
            stream.seek(20)
            assertEquals(241, stream.readUnsignedByte())

            stream.seek(0)
            stream.order = ByteOrder.LITTLE_ENDIAN
            assertEquals(1, stream.readUnsignedByte())
            assertEquals(1, stream.position())
            stream.seek(20)
            assertEquals(241, stream.readUnsignedByte())
        }
    }

    @Test
    fun readUTFBE() {
        val text = "xyz\nüabcd\n"
        val data = byteArrayOf(0, 7.toByte(), *text.toByteArray())
        EndianAwareDataSeekableStream(ByteArraySeekableStream(data)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            assertEquals("xyz\nüa", stream.readUTF())
            assertEquals(9, stream.position())
        }
    }

    @Test
    fun readUTFLE() {
        val text = "xyz\nüabcd\n"
        val data = byteArrayOf(0, 7.toByte(), *text.toByteArray())
        EndianAwareDataSeekableStream(ByteArraySeekableStream(data)).use { stream ->
            stream.order = ByteOrder.LITTLE_ENDIAN
            assertEquals("xyz\nüa", stream.readUTF())
            assertEquals(9, stream.position())
        }
    }

    @Test
    fun readChar() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            Assert.assertArrayEquals(byteArrayOf(1, 3), Chars.toByteArray(stream.readChar()))
            assertEquals(2, stream.position())
            Assert.assertArrayEquals(byteArrayOf(4, 7), Chars.toByteArray(stream.readChar()))

            stream.seek(0)
            stream.order = ByteOrder.LITTLE_ENDIAN
            Assert.assertArrayEquals(byteArrayOf(3, 1), Chars.toByteArray(stream.readChar()))
            assertEquals(2, stream.position())
            Assert.assertArrayEquals(byteArrayOf(7, 4), Chars.toByteArray(stream.readChar()))
        }
    }

    @Test(expected = UnsupportedOperationException::class)
    fun readLineBE() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            @Suppress("DEPRECATION")
            stream.readLine()
        }
    }

    @Test(expected = UnsupportedOperationException::class)
    fun readLineLE() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.LITTLE_ENDIAN
            @Suppress("DEPRECATION")
            stream.readLine()
        }
    }

    @Test
    fun readByte() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            assertEquals(1.toByte(), stream.readByte())
            assertEquals(1, stream.position())
            assertEquals(3.toByte(), stream.readByte())

            stream.seek(0)
            stream.order = ByteOrder.LITTLE_ENDIAN
            assertEquals(1.toByte(), stream.readByte())
            assertEquals(1, stream.position())
            assertEquals(3.toByte(), stream.readByte())
        }
    }

    @Test
    fun skipBytes() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            assertEquals(1.toByte(), stream.readByte())
            stream.skipBytes(5)
            assertEquals(6, stream.position())
            assertEquals(127.toByte(), stream.readByte())

            stream.seek(0)
            stream.order = ByteOrder.LITTLE_ENDIAN
            assertEquals(1.toByte(), stream.readByte())
            stream.skipBytes(5)
            assertEquals(6, stream.position())
            assertEquals(127.toByte(), stream.readByte())
        }
    }


    @Test
    fun readBoolean() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            assertTrue(stream.readBoolean())
            assertEquals(1, stream.position())
            stream.seek(1)
            assertTrue(stream.readBoolean())
            stream.seek(5)
            assertFalse(stream.readBoolean())

            stream.order = ByteOrder.LITTLE_ENDIAN
            stream.seek(0)
            assertTrue(stream.readBoolean())
            assertEquals(1, stream.position())
            stream.seek(1)
            assertTrue(stream.readBoolean())
            stream.seek(5)
            assertFalse(stream.readBoolean())
        }
    }

    @Test
    fun readShort() {
        EndianAwareDataSeekableStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            stream.order = ByteOrder.BIG_ENDIAN
            stream.seek(0)
            assertEquals(259.toShort(), stream.readShort())
            assertEquals(2, stream.position())
            assertEquals(1031.toShort(), stream.readShort())
            stream.seek(20)
            assertEquals((-3600).toShort(), stream.readShort())

            stream.seek(0)
            stream.order = ByteOrder.LITTLE_ENDIAN
            assertEquals(769.toShort(), stream.readShort())
            assertEquals(2, stream.position())
            assertEquals(1796.toShort(), stream.readShort())
            stream.seek(20)
            assertEquals((-3855).toShort(), stream.readShort())
        }
    }

    companion object {
        val TEST_DATA = byteArrayOf(1, 3, 4, 7, 9, 0, Byte.MAX_VALUE, 34, -1, Byte.MIN_VALUE, 100,
                                    101, 102, 103, 104, -10, -11, -12, -13, -14, -15,
                                    -16, -17, -18, '\n'.toByte(), 11, 12)
    }
}