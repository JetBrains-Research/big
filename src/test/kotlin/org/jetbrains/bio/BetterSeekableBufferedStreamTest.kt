package org.jetbrains.bio

import htsjdk.samtools.seekablestream.ByteArraySeekableStream
import org.junit.Assert
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * @author Roman.Chernyatchik
 */
class BetterSeekableBufferedStreamTest {
    @get:Rule
    var expectedEx = ExpectedException.none()

    @Test
    fun defaultValues() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            assertEquals(128000, stream.bufferSize)
            assertEquals(0, stream.bufferStartOffset)
            assertEquals(0, stream.bufferEndOffset)
            assertEquals(0, stream.position)
            assertEquals(0, stream.position())
            Assert.assertArrayEquals(ByteArray(128000), stream.buffer!!)
        }
    }

    @Test
    fun readFromBegin() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            assertEquals(1, stream.read())
            assertEquals(3, stream.read())

            assertEquals(0, stream.bufferStartOffset)
            assertEquals(10, stream.bufferEndOffset)
        }
    }

    @Test
    fun readUpToEof() {
        val buff = mutableListOf<Int>()
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            while (!stream.eof()) {
                buff.add(stream.read())
            }
        }
        Assert.assertEquals(TEST_DATA.map { it.toInt() and 0xFF }, buff)
        Assert.assertArrayEquals(TEST_DATA, buff.map { it.toByte() }.toByteArray())
    }

    @Test
    fun readUpToMinusOne() {
        val buff = mutableListOf<Int>()
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            var nextByte = stream.read()
            while (nextByte != -1) {
                buff.add(nextByte)
                nextByte = stream.read()
            }

            assertEquals(-1, stream.read())
            assertEquals(-1, stream.read())
            assertEquals(-1, stream.read())
            assertTrue(stream.eof())
        }
        Assert.assertEquals(TEST_DATA.map { it.toInt() and 0xFF }, buff)
        Assert.assertArrayEquals(TEST_DATA, buff.map { it.toByte() }.toByteArray())
    }

    @Test
    fun seek() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            assertEquals(0, stream.position)

            stream.seek(3)
            assertEquals(3, stream.position)
            assertEquals(0, stream.bufferStartOffset)
            assertEquals(0, stream.bufferEndOffset)

            stream.seek(13)
            assertEquals(13, stream.position)
            assertEquals(0, stream.bufferStartOffset)
            assertEquals(0, stream.bufferEndOffset)
        }
    }

    @Test
    fun seekOutOfBounds() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            val oobPos = TEST_DATA.size + 100L
            stream.seek(oobPos)
            assertEquals(oobPos, stream.position())
            assertEquals(oobPos, stream.position)
        }
    }

    @Test
    fun seekNegative() {
        expectedEx.expect(IllegalArgumentException::class.java)
        expectedEx.expectMessage("Position should be non-negative value, but was -1")

        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            stream.seek(3)
            stream.seek(-1)
        }
    }

    @Test
    fun readAndClose() {
        expectedEx.expect(IllegalStateException::class.java)
        expectedEx.expectMessage("Stream is closed")

        val stream = BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10)
        assertFalse(stream.eof())

        stream.close()
        assertEquals(0, stream.position)

        stream.read()
    }

    @Test
    fun closeAndSeek() {
        val stream = BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10)
        assertFalse(stream.eof())

        stream.close()
        assertEquals(0, stream.position)

        stream.seek(10)
        assertEquals(10, stream.position)
    }

    @Test
    fun close() {
        expectedEx.expect(IllegalStateException::class.java)
        expectedEx.expectMessage("Stream is closed")

        val stream = BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10)
        stream.read()
        assertFalse(stream.eof())

        stream.close()
        assertEquals(1, stream.position)

        stream.read()
    }

    @Test(expected = NullPointerException::class)
    fun closeAndEof() {
        val stream = BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10)
        stream.read()
        assertFalse(stream.eof())

        stream.close()
        stream.eof()
    }

    @Test
    fun getSource() {
        val byteStream = ByteArraySeekableStream(TEST_DATA)
        BetterSeekableBufferedStream(byteStream).use { stream ->
            assertEquals(byteStream.source, stream.source)
        }
    }

    @Test
    fun getLength() {
        val byteStream = ByteArraySeekableStream(TEST_DATA)
        BetterSeekableBufferedStream(byteStream).use { stream ->
            assertEquals(TEST_DATA.size, stream.length().toInt())
        }
    }

    @Test
    fun getPosition() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            assertEquals(0, stream.position())

            stream.seek(3)
            assertEquals(3, stream.position())
            assertEquals(3, stream.position)

            stream.read()
            assertEquals(4, stream.position())
            assertEquals(4, stream.position)
        }
    }

    @Test
    fun readAfterCachedEnd() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            stream.read()
            stream.seek(14)
            assertEquals(104, stream.read())
            assertEquals(15, stream.position)
            assertEquals(14, stream.bufferStartOffset)
            assertEquals(24, stream.bufferEndOffset)
        }
    }

    @Test
    fun readTrimmed() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            stream.read()
            stream.seek((TEST_DATA.size - 4).toLong())
            assertEquals(TEST_DATA[TEST_DATA.size - 4].toInt() and 0xFF, stream.read())
            assertEquals((TEST_DATA.size - 3).toLong(), stream.position)
            assertEquals((TEST_DATA.size - 4).toLong(), stream.bufferStartOffset)
            assertEquals(TEST_DATA.size.toLong(), stream.bufferEndOffset)
        }
    }

    @Test
    fun readBeforeCachedStartIntersectingWithCache() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            stream.seek(14)
            stream.read()

            stream.seek(10)
            assertEquals(100, stream.read())
            assertEquals(11, stream.position)
            assertEquals(10, stream.bufferStartOffset)
            assertEquals(20, stream.bufferEndOffset)

            assertEquals(
                    TEST_DATA.toList().subList(stream.bufferStartOffset.toInt(), stream.bufferEndOffset.toInt()),
                    stream.buffer!!.toList()
            )
        }
    }

    @Test
    fun readBeforeCachedStart() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10).use { stream ->
            stream.seek(14)
            stream.read()

            stream.seek(3)
            assertEquals(7, stream.read())
            assertEquals(4, stream.position)
            assertEquals(3, stream.bufferStartOffset)
            assertEquals(13, stream.bufferEndOffset)

            assertEquals(
                    TEST_DATA.toList().subList(stream.bufferStartOffset.toInt(), stream.bufferEndOffset.toInt()),
                    stream.buffer!!.toList()
            )
        }
    }

    @Test
    fun fillInCache() {
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10) {
            fun fillBuffer_() {
                fillBuffer()
            }
        }.use { stream ->
            stream.seek(10)
            stream.read()

            stream.seek(13)
            stream.fillBuffer_()

            assertEquals(10, stream.bufferStartOffset)
            assertEquals(20, stream.bufferEndOffset)

            assertEquals(103, stream.read())
            assertEquals(14, stream.position)

            assertEquals(
                    TEST_DATA.toList().subList(stream.bufferStartOffset.toInt(), stream.bufferEndOffset.toInt()),
                    stream.buffer!!.toList()
            )
        }
    }

    @Test
    fun fillInCacheTrimmed() {
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10) {

            fun fillBuffer_() {
                fillBuffer()
            }
        }.use { stream ->
            // fill whole buffer:
            stream.seek(0L)
            stream.read()
            // force trimmed buffer rewritten
            stream.seek((TEST_DATA.size - 6).toLong())
            stream.read()

            stream.seek((TEST_DATA.size - 4).toLong())
            stream.fillBuffer_()

            assertEquals((TEST_DATA.size - 6).toLong(), stream.bufferStartOffset)
            assertEquals(TEST_DATA.size.toLong(), stream.bufferEndOffset)

            assertEquals(TEST_DATA[TEST_DATA.size - 4].toInt() and 0xFF, stream.read())
            assertEquals((TEST_DATA.size - 3).toLong(), stream.position)

            assertEquals(
                    TEST_DATA.toList().subList(stream.bufferStartOffset.toInt(), stream.bufferEndOffset.toInt()),
                    stream.buffer!!.toList().subList(0, 6)
            )
            // buffer size > available bytes => some garbage in the end of buffer
            assertEquals(
                    listOf(-13, -14, -15, -16, -17, -18, 0, 0, 0, 0),
                    stream.buffer!!.map { it.toInt() }
            )
        }
    }

    @Test
    fun readArrayAfterCachedStart() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(5)
            stream.read(buffer, 0, 3)
            assertEquals(8, stream.position)
            assertEquals(5, stream.bufferStartOffset)
            assertEquals(10, stream.bufferEndOffset)

            assertEquals(TEST_DATA.toList().subList(5, 8), buffer.toList().subList(0, 3))
        }
    }

    @Test
    fun readArrayAfterCachedStartLong() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(5)
            stream.read(buffer, 0, buffer.size)
            assertEquals(15, stream.position)
            assertEquals(10, stream.bufferStartOffset)
            assertEquals(15, stream.bufferEndOffset)

            assertEquals(TEST_DATA.toList().subList(5, 15), buffer.toList().subList(0, 10))
        }
    }

    @Test
    fun readArrayAfterInCachedPart() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(4)
            stream.read(buffer, 0, 3)
            assertEquals(7, stream.position)
            assertEquals(5, stream.bufferStartOffset)
            assertEquals(10, stream.bufferEndOffset)

            assertEquals(TEST_DATA.toList().subList(4, 7), buffer.toList().subList(0, 3))
        }
    }

    @Test
    fun readArrayAfterInCachedPartLong() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(4)
            stream.read(buffer, 0, 10)
            assertEquals(14, stream.position)
            assertEquals(10, stream.bufferStartOffset)
            assertEquals(15, stream.bufferEndOffset)

            assertEquals(TEST_DATA.toList().subList(4, 14), buffer.toList().subList(0, 10))
        }
    }

    @Test
    fun readArrayAfterInCachedPartTrimmed() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(100)
            stream.seek(4)
            stream.read(buffer, 0, 100)
            assertEquals(24, stream.position)
            assertEquals(0, stream.bufferStartOffset)
            assertEquals(-1, stream.bufferEndOffset)
            assertTrue(stream.eof())

            assertEquals(TEST_DATA.toList().subList(4, 24), buffer.toList().subList(0, 20))
        }
    }

    @Test
    fun readArrayBeforeCachedPart() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(3)
            stream.seek(1)
            stream.read(buffer, 0, 3)
            assertEquals(4, stream.position)
            assertEquals(1, stream.bufferStartOffset)
            assertEquals(6, stream.bufferEndOffset)

            assertEquals(TEST_DATA.toList().subList(1, 4), buffer.toList().subList(0, 3))
        }
    }

    @Test
    fun readArrayBeforeCachedPartLONG() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(1)
            stream.read(buffer, 0, 8)
            assertEquals(9, stream.position)
            assertEquals(6, stream.bufferStartOffset)
            assertEquals(11, stream.bufferEndOffset)

            assertEquals(TEST_DATA.toList().subList(1, 9), buffer.toList().subList(0, 8))
        }
    }

    @Test
    fun readArrayBeforeCachedPartIntersectingCache() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(8)
            stream.read(buffer, 0, 3)
            assertEquals(11, stream.position)
            assertEquals(8, stream.bufferStartOffset)
            assertEquals(13, stream.bufferEndOffset)

            assertEquals(TEST_DATA.toList().subList(8, 11), buffer.toList().subList(0, 3))
        }
    }

    @Test
    fun readArrayBeforeCachedPartIntersectingCacheLong() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(3)
            stream.read(buffer, 0, 8)
            assertEquals(11, stream.position)
            assertEquals(8, stream.bufferStartOffset)
            assertEquals(13, stream.bufferEndOffset)

            assertEquals(TEST_DATA.toList().subList(3, 11), buffer.toList().subList(0, 8))
        }
    }

    @Test
    fun readArrayOverlapCachedPart() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(8)
            stream.read(buffer, 0, 10)
            assertEquals(18, stream.position)
            assertEquals(13, stream.bufferStartOffset)
            assertEquals(18, stream.bufferEndOffset)

            assertEquals(TEST_DATA.toList().subList(8, 18), buffer.toList().subList(0, 10))
        }
    }

    @Test
    fun readArrayFromOffsetOverlapCachedPart() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(13)
            stream.seek(8)
            stream.read(buffer, 3, 10)
            assertEquals(18, stream.position)
            assertEquals(13, stream.bufferStartOffset)
            assertEquals(18, stream.bufferEndOffset)

            assertEquals(listOf<Byte>(0, 0, 0), buffer.toList().subList(0, 3))
            assertEquals(TEST_DATA.toList().subList(8, 18), buffer.toList().subList(3, 13))
        }
    }

    @Test
    fun readArrayFromOffsetAfterInCachedPart() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(4)
            stream.read(buffer, 3, 3)
            assertEquals(7, stream.position)
            assertEquals(5, stream.bufferStartOffset)
            assertEquals(10, stream.bufferEndOffset)

            assertEquals(listOf<Byte>(0, 0, 0), buffer.toList().subList(0, 3))
            assertEquals(TEST_DATA.toList().subList(4, 7), buffer.toList().subList(3, 6))
        }
    }

    @Test
    fun readArrayFromOffsetBeforeCachedPartIntersectingCache() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(8)
            stream.read(buffer, 3, 3)
            assertEquals(11, stream.position)
            assertEquals(8, stream.bufferStartOffset)
            assertEquals(13, stream.bufferEndOffset)

            assertEquals(listOf<Byte>(0, 0, 0), buffer.toList().subList(0, 3))
            assertEquals(TEST_DATA.toList().subList(8, 11), buffer.toList().subList(3, 6))
        }
    }

    @Test
    fun switchBuffersIfNeeded() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 4).use { stream ->
            stream.seek(2)
            stream.read()

            assertEquals(1, stream.curBufIdx())

            // new buffer intersect buffer's start
            stream.seek(0)
            stream.switchBuffersIfNeeded()
            assertEquals(1, stream.curBufIdx())

            // new buffer intersect buffer's end
            stream.seek(2 + 4 - 1)
            assertEquals(1, stream.curBufIdx())

            // no intersection
            stream.seek(13)
            stream.switchBuffersIfNeeded()
            assertEquals(0, stream.curBufIdx())
        }
    }

    @Test
    fun fillAndSwitchBuffersIfNeeded() {
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 4) {
            fun fillBuffer_() {
                fillBuffer()
            }
        }.use { stream ->
            stream.seek(2)
            stream.fillBuffer_()

            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(0L, 2L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(0L, 6L), stream.bufferEndOffsets)
            Assert.assertArrayEquals(byteArrayOf(0, 0, 0, 0), stream.buffers[0])
            Assert.assertArrayEquals(byteArrayOf(4, 7, 9, 12), stream.buffers[1])

            // new buffer intersect buffer's start
            stream.seek(0)
            stream.fillBuffer_()
            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(0L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(0L, 4L), stream.bufferEndOffsets)
            Assert.assertArrayEquals(byteArrayOf(0, 0, 0, 0), stream.buffers[0])
            Assert.assertArrayEquals(byteArrayOf(1, 3, 4, 7), stream.buffers[1])

            // new buffer intersect buffer's end
            stream.seek(3)
            stream.fillBuffer_()
            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(0L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(0L, 4L), stream.bufferEndOffsets)
            Assert.assertArrayEquals(byteArrayOf(0, 0, 0, 0), stream.buffers[0])
            Assert.assertArrayEquals(byteArrayOf(1, 3, 4, 7), stream.buffers[1])

            // no intersection
            stream.seek(13)
            stream.fillBuffer_()
            assertEquals(0, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(13L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(17L, 4L), stream.bufferEndOffsets)
            Assert.assertArrayEquals(byteArrayOf(103, 104, -10, -11), stream.buffers[0])
            Assert.assertArrayEquals(byteArrayOf(1, 3, 4, 7), stream.buffers[1])

            stream.seek(3)
            stream.fillBuffer_()
            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(13L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(17L, 4L), stream.bufferEndOffsets)
            Assert.assertArrayEquals(byteArrayOf(103, 104, -10, -11), stream.buffers[0])
            Assert.assertArrayEquals(byteArrayOf(1, 3, 4, 7), stream.buffers[1])

            stream.seek(18)
            stream.fillBuffer_()
            assertEquals(0, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(18L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(22L, 4L), stream.bufferEndOffsets)
            Assert.assertArrayEquals(byteArrayOf(-13, -14, -15, -16), stream.buffers[0])
            Assert.assertArrayEquals(byteArrayOf(1, 3, 4, 7), stream.buffers[1])
        }
    }


    @Test
    fun readAndSwitchBuffersIfNeeded() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 4).use { stream ->
            stream.seek(2)
            stream.read()

            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(0L, 2L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(0L, 6L), stream.bufferEndOffsets)
            Assert.assertArrayEquals(byteArrayOf(0, 0, 0, 0), stream.buffers[0])
            Assert.assertArrayEquals(byteArrayOf(4, 7, 9, 12), stream.buffers[1])

            // new buffer intersect buffer's start
            stream.seek(0)
            stream.read()
            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(0L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(0L, 4L), stream.bufferEndOffsets)
            Assert.assertArrayEquals(byteArrayOf(0, 0, 0, 0), stream.buffers[0])
            Assert.assertArrayEquals(byteArrayOf(1, 3, 4, 7), stream.buffers[1])

            // new buffer intersect buffer's end
            stream.seek(3)
            stream.read()
            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(0L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(0L, 4L), stream.bufferEndOffsets)
            Assert.assertArrayEquals(byteArrayOf(0, 0, 0, 0), stream.buffers[0])
            Assert.assertArrayEquals(byteArrayOf(1, 3, 4, 7), stream.buffers[1])

            // no intersection
            stream.seek(13)
            stream.read()
            assertEquals(0, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(13L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(17L, 4L), stream.bufferEndOffsets)
            Assert.assertArrayEquals(byteArrayOf(103, 104, -10, -11), stream.buffers[0])
            Assert.assertArrayEquals(byteArrayOf(1, 3, 4, 7), stream.buffers[1])

            stream.seek(2)
            stream.read()
            assertEquals(1, stream.curBufIdx())
        }
    }

    @Test
    fun fetchZeroBuffer() {
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 4) {
            fun fetchNewBuffer_(pos: Long, buffOffset: Int, size: Int) {
                fetchNewBuffer(pos, buffOffset, size)
            }
        }.use { stream ->
            stream.seek(2)
            stream.read()
            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(0L, 2L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(0L, 6L), stream.bufferEndOffsets)

            stream.fetchNewBuffer_(6, 1, 0)
            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(0L, 6L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(0L, 6L), stream.bufferEndOffsets)
        }
    }
    companion object {
        val TEST_DATA = byteArrayOf(1, 3, 4, 7, 9, 12, Byte.MAX_VALUE, 34, -1, Byte.MIN_VALUE,
                                    100, 101, 102, 103, 104, -10, -11, -12, -13, -14, -15, -16, -17, -18)
    }
}

