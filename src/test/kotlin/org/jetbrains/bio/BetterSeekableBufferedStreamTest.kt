package org.jetbrains.bio

import htsjdk.samtools.seekablestream.ByteArraySeekableStream
import org.jetbrains.bio.BetterSeekableBufferedStreamTest.Companion.TEST_DATA
import org.jetbrains.bio.BetterSeekableBufferedStreamTest.Companion.assertBufferMatchesRealData
import org.jetbrains.bio.BetterSeekableBufferedStreamTest.Companion.assertCacheMatchesRealData
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * @author Roman.Chernyatchik
 */
@RunWith(Parameterized::class)
class BetterSeekableBufferedStreamTestParametrized(
        private val doubleBuffer: Boolean
) {
    @Test
    fun readFromBegin() {
        createStream(10).use { stream ->
            assertEquals(1, stream.read())
            assertEquals(3, stream.read())

            assertCacheMatchesRealData(0L to 10L, stream)
        }
    }

    @Test
    fun readUpToEof() {
        val buff = mutableListOf<Int>()
        createStream(10).use { stream ->
            while (!stream.eof()) {
                buff.add(stream.read())
            }
        }
        Assert.assertEquals(BetterSeekableBufferedStreamTest.TEST_DATA.map { it.toInt() and 0xFF }, buff)
        Assert.assertArrayEquals(BetterSeekableBufferedStreamTest.TEST_DATA, buff.map { it.toByte() }.toByteArray())
    }

    @Test
    fun readUpToMinusOne() {
        val buff = mutableListOf<Int>()
        createStream(10).use { stream ->
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
        Assert.assertEquals(BetterSeekableBufferedStreamTest.TEST_DATA.map { it.toInt() and 0xFF }, buff)
        Assert.assertArrayEquals(BetterSeekableBufferedStreamTest.TEST_DATA, buff.map { it.toByte() }.toByteArray())
    }

    @Test
    fun seek() {
        createStream(10).use { stream ->
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
        createStream(10).use { stream ->
            val oobPos = BetterSeekableBufferedStreamTest.TEST_DATA.size + 100L
            stream.seek(oobPos)
            assertEquals(oobPos, stream.position())
            assertEquals(oobPos, stream.position)
        }
    }

    @Test
    fun seekNegative() {
        val ex = Assert.assertThrows(IllegalArgumentException::class.java) {
            val bufferSize = 10
            createStream(bufferSize, "test data").use { stream ->
                stream.seek(3)
                stream.seek(-1)
            }
        }
        assertEquals("Position should be non-negative value, but was -1 in test data", ex.message)
    }

    @Test
    fun seekNegativeStreamWoSource() {
        val ex = Assert.assertThrows(IllegalArgumentException::class.java) {
            val bufferSize = 10
            createStream(bufferSize, null).use { stream ->
                stream.seek(3)
                stream.seek(-1)
            }
        }
        assertEquals("Position should be non-negative value, but was -1 in null", ex.message)
    }

    @Test
    fun readAndClose() {
        val ex = Assert.assertThrows(IllegalStateException::class.java) {
            val stream = createStream(10, "test data")
            assertFalse(stream.eof())

            stream.close()
            assertEquals(0, stream.position)

            stream.read()
        }
        assertEquals("Stream is closed: test data", ex.message)
    }

    @Test
    fun closeAndSeek() {
        val stream = createStream(10)
        assertFalse(stream.eof())

        stream.close()
        assertEquals(0, stream.position)

        stream.seek(10)
        assertEquals(10, stream.position)
    }

    @Test
    fun close() {
        val ex = Assert.assertThrows(IllegalStateException::class.java) {
            val stream = createStream(10, "test data")
            stream.read()
            assertFalse(stream.eof())

            stream.close()
            assertEquals(1, stream.position)

            stream.read()
        }
        assertEquals("Stream is closed: test data", ex.message)
    }

    @Test
    fun closeStreamWoSource() {
        val ex = Assert.assertThrows(IllegalStateException::class.java) {
            val stream = createStream(10, null)
            stream.read()
            assertFalse(stream.eof())

            stream.close()
            assertEquals(1, stream.position)

            stream.read()
        }
        assertEquals("Stream is closed: null", ex.message)
    }

    @Test(expected = NullPointerException::class)
    fun closeAndEof() {
        val stream = createStream(10)
        stream.read()
        assertFalse(stream.eof())

        stream.close()
        stream.eof()
    }

    @Test
    fun getPosition() {
        createStream(10).use { stream ->
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
        createStream(10).use { stream ->
            stream.read()
            stream.seek(14)
            assertEquals(104, stream.read())
            assertEquals(15, stream.position)
            assertCacheMatchesRealData(14L to 24L, stream)
        }
    }

    @Test
    fun readTrimmed() {
        createStream(10).use { stream ->
            stream.read()

            val n = BetterSeekableBufferedStreamTest.TEST_DATA.size.toLong()
            stream.seek(n - 4)
            assertEquals(
                    BetterSeekableBufferedStreamTest.TEST_DATA[n.toInt() - 4].toInt() and 0xFF,
                    stream.read()
            )
            assertEquals(n - 3, stream.position)
            assertCacheMatchesRealData(n - 4 to n, stream)
        }
    }

    @Test
    fun readBeforeCachedStartIntersectingWithCache() {
        createStream(10).use { stream ->
            stream.seek(14)
            stream.read()

            stream.seek(10)
            assertEquals(100, stream.read())
            assertEquals(11, stream.position)

            assertCacheMatchesRealData(10L to 20L, stream)
        }
    }

    @Test
    fun readBeforeCachedStart() {
        createStream(10).use { stream ->
            stream.seek(14)
            stream.read()

            stream.seek(3)
            assertEquals(7, stream.read())
            assertEquals(4, stream.position)
            assertCacheMatchesRealData(3L to 13L, stream)
        }
    }

    @Test
    fun fillInCache() {
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10, doubleBuffer) {
            fun fillBuffer_() {
                fillBuffer()
            }
        }.use { stream ->
            stream.seek(10)
            stream.read()

            stream.seek(13)
            stream.fillBuffer_()

            assertCacheMatchesRealData(10L to 20L, stream)
            assertEquals(103, stream.read())
            assertEquals(14, stream.position)
            assertCacheMatchesRealData(10L to 20L, stream)
        }
    }

    @Test
    fun fillInCacheTrimmed() {
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 10, doubleBuffer) {
            fun fillBuffer_() {
                fillBuffer()
            }
        }.use { stream ->
            // fill whole buffer:
            stream.seek(0L)
            stream.read()
            // force trimmed buffer rewritten
            stream.seek((BetterSeekableBufferedStreamTest.TEST_DATA.size - 6).toLong())
            stream.read()

            stream.seek((BetterSeekableBufferedStreamTest.TEST_DATA.size - 4).toLong())
            stream.fillBuffer_()

            assertEquals((BetterSeekableBufferedStreamTest.TEST_DATA.size - 6).toLong(), stream.bufferStartOffset)
            assertEquals(BetterSeekableBufferedStreamTest.TEST_DATA.size.toLong(), stream.bufferEndOffset)

            assertEquals(BetterSeekableBufferedStreamTest.TEST_DATA[BetterSeekableBufferedStreamTest.TEST_DATA.size - 4].toInt() and 0xFF, stream.read())
            assertEquals((BetterSeekableBufferedStreamTest.TEST_DATA.size - 3).toLong(), stream.position)

            assertCacheMatchesRealData(18L to 24L, stream)
            // buffer size > available bytes => some garbage in the end of buffer
            assertEquals(
                    when {
                        doubleBuffer -> listOf(-13, -14, -15, -16, -17, -18, 0, 0, 0, 0)
                        else -> listOf(-13, -14, -15, -16, -17, -18, 127, 34, -1, -128)
                    },
                    stream.buffer!!.map { it.toInt() }
            )
        }
    }

    @Test
    fun readArrayAfterCachedStart() {
        createStream(5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(5)
            stream.read(buffer, 0, 3)
            assertEquals(8, stream.position)
            assertEquals(5, stream.bufferStartOffset)
            assertEquals(10, stream.bufferEndOffset)

            assertBufferMatchesRealData(5 to 8, buffer)
        }
    }

    @Test
    fun readArrayAfterCachedStartLong() {
        createStream(5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(5)
            stream.read(buffer, 0, buffer.size)
            assertEquals(15, stream.position)
            assertEquals(10, stream.bufferStartOffset)
            assertEquals(15, stream.bufferEndOffset)

            assertBufferMatchesRealData(5 to 15, buffer)
        }
    }

    @Test
    fun readArrayAfterInCachedPart() {
        createStream(5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(4)
            stream.read(buffer, 0, 3)
            assertEquals(7, stream.position)
            assertEquals(5, stream.bufferStartOffset)
            assertEquals(10, stream.bufferEndOffset)

            assertBufferMatchesRealData(4 to 7, buffer)
        }
    }

    @Test
    fun readArrayAfterInCachedPartLong() {
        createStream(5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(4)
            stream.read(buffer, 0, 10)
            assertEquals(14, stream.position)
            assertEquals(10, stream.bufferStartOffset)
            assertEquals(15, stream.bufferEndOffset)

            assertBufferMatchesRealData(4 to 14, buffer)
        }
    }

    @Test
    fun readArrayAfterInCachedPartTrimmed() {
        createStream(5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(100)
            stream.seek(4)
            stream.read(buffer, 0, 100)
            assertEquals(24, stream.position)
            assertEquals(0, stream.bufferStartOffset)
            assertEquals(-1, stream.bufferEndOffset)
            assertTrue(stream.eof())

            assertBufferMatchesRealData(4 to 24, buffer)
        }
    }

    @Test
    fun readArrayBeforeCachedPart() {
        createStream(5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(3)
            stream.seek(1)
            stream.read(buffer, 0, 3)
            assertEquals(4, stream.position)
            assertEquals(1, stream.bufferStartOffset)
            assertEquals(6, stream.bufferEndOffset)

            assertBufferMatchesRealData(1 to 4, buffer)
        }
    }

    @Test
    fun readArrayBeforeCachedPartLONG() {
        createStream(5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(1)
            stream.read(buffer, 0, 8)
            assertEquals(9, stream.position)
            assertEquals(6, stream.bufferStartOffset)
            assertEquals(11, stream.bufferEndOffset)

            assertBufferMatchesRealData(1 to 9, buffer)
        }
    }

    @Test
    fun readArrayBeforeCachedPartIntersectingCache() {
        createStream(5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(8)
            stream.read(buffer, 0, 3)
            assertEquals(11, stream.position)
            assertEquals(8, stream.bufferStartOffset)
            assertEquals(13, stream.bufferEndOffset)

            assertBufferMatchesRealData(8 to 11, buffer)
        }
    }

    @Test
    fun readArrayBeforeCachedPartIntersectingCacheLong() {
        createStream(5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(3)
            stream.read(buffer, 0, 8)
            assertEquals(11, stream.position)
            assertEquals(8, stream.bufferStartOffset)
            assertEquals(13, stream.bufferEndOffset)

            assertBufferMatchesRealData(3 to 11, buffer)
        }
    }

    @Test
    fun readArrayOverlapCachedPart() {
        createStream(5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(8)
            stream.read(buffer, 0, 10)
            assertEquals(18, stream.position)
            assertEquals(13, stream.bufferStartOffset)
            assertEquals(18, stream.bufferEndOffset)

            assertBufferMatchesRealData(8 to 18, buffer)
        }
    }

    @Test
    fun readArrayFromOffsetOverlapCachedPart() {
        createStream(5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(13)
            stream.seek(8)
            stream.read(buffer, 3, 10)
            assertEquals(18, stream.position)
            assertEquals(13, stream.bufferStartOffset)
            assertEquals(18, stream.bufferEndOffset)

            assertEquals(listOf<Byte>(0, 0, 0), buffer.toList().subList(0, 3))
            assertBufferMatchesRealData(8 to 18, buffer, 3)
        }
    }

    @Test
    fun readArrayFromOffsetAfterInCachedPart() {
        createStream(5).use { stream ->
            stream.seek(0)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(4)
            stream.read(buffer, 3, 3)
            assertEquals(7, stream.position)
            assertEquals(5, stream.bufferStartOffset)
            assertEquals(10, stream.bufferEndOffset)

            assertEquals(listOf<Byte>(0, 0, 0), buffer.toList().subList(0, 3))
            assertBufferMatchesRealData(4 to 7, buffer, 3)
        }
    }

    @Test
    fun readArrayFromOffsetBeforeCachedPartIntersectingCache() {
        createStream(5).use { stream ->
            stream.seek(10)
            stream.read()

            val buffer = ByteArray(10)
            stream.seek(8)
            stream.read(buffer, 3, 3)
            assertEquals(11, stream.position)
            assertEquals(8, stream.bufferStartOffset)
            assertEquals(13, stream.bufferEndOffset)

            assertEquals(listOf<Byte>(0, 0, 0), buffer.toList().subList(0, 3))
            assertBufferMatchesRealData(8 to 11, buffer, 3)
        }
    }

    private fun createStream(bufferSize: Int, source: String? = null): BetterSeekableBufferedStream =
        BetterSeekableBufferedStream(
            object : ByteArraySeekableStream(BetterSeekableBufferedStreamTest.TEST_DATA) {
                override fun getSource() = source
            }, bufferSize, doubleBuffer
        )

    companion object {
        @Parameterized.Parameters(name = "{0}")
        @JvmStatic
        fun data() = listOf(arrayOf(true), arrayOf(false))
    }

}

class BetterSeekableBufferedStreamTest {
    @Test
    fun defaultValues() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA)).use { stream ->
            assertTrue(stream.doubleBuffer)
            assertEquals(128000, stream.bufferSize)
            assertEquals(0, stream.bufferStartOffset)
            Assert.assertArrayEquals(arrayOf(0L, 0L), stream.bufferStartOffsets)
            assertEquals(0, stream.bufferEndOffset)
            Assert.assertArrayEquals(arrayOf(0L, 0L), stream.bufferEndOffsets)
            assertEquals(0, stream.position)
            assertEquals(0, stream.position())
            Assert.assertArrayEquals(ByteArray(128000), stream.buffer!!)
        }
    }

    @Test
    fun getSource() {
        val byteStream = ByteArraySeekableStream(BetterSeekableBufferedStreamTest.TEST_DATA)
        BetterSeekableBufferedStream(byteStream).use { stream ->
            assertEquals(byteStream.source, stream.source)
        }
    }

    @Test
    fun getLength() {
        val byteStream = ByteArraySeekableStream(BetterSeekableBufferedStreamTest.TEST_DATA)
        BetterSeekableBufferedStream(byteStream).use { stream ->
            assertEquals(BetterSeekableBufferedStreamTest.TEST_DATA.size, stream.length().toInt())
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
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 4).use { stream ->
            stream.seek(2)
            stream.read()
            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(0L, 2L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(0L, 6L), stream.bufferEndOffsets)
            assertCacheMatchesRealData(2L to 6L, stream)

            assertEquals(0, stream.fetchNewBuffer(6, 1, 0))
            assertEquals(1, stream.curBufIdx())
            Assert.assertArrayEquals(arrayOf(0L, 6L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(0L, 6L), stream.bufferEndOffsets)
            assertCacheMatchesRealData(6L to 6L, stream)
        }
    }

    @Test
    fun fetchFullBuffer() {
        BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 6, false).use { stream ->
            stream.seek(2)
            stream.read()
            assertEquals(4, stream.fetchNewBuffer(6, 1, 4))
        }
    }

    @Test
    fun fetchFullBufferTrimmed() {
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 6) {
            override fun fetchNewBuffer(pos: Long, buffOffset: Int, size: Int) = super.fetchNewBuffer(
                    pos, buffOffset,
                    if (pos == 6L) 2 else size
            )
        }.use { stream ->
            stream.seek(2)
            stream.read()
            assertEquals(2, stream.fetchNewBuffer(6, 1, 4))
        }
    }

    @Test
    fun fetchNewBufferTrimmedIntersectingCache() {
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 6) {
            override fun fetchNewBuffer(pos: Long, buffOffset: Int, size: Int) = super.fetchNewBuffer(
                    pos, buffOffset,
                    if (pos == 14L) 2 else size
            )
        }.use { stream ->
            // fill whole buffer
            stream.seek(7) // 7..13
            stream.read()
            // fill 2nd buffer in case of double buffer
            stream.seek(0) // 0..6
            stream.read()
            // validate
            Assert.assertArrayEquals(arrayOf(0L, 7L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(6L, 13L), stream.bufferEndOffsets)

            // read next buffer but less than buffer size
            stream.seek(14) // 14..16
            stream.read()
            Assert.assertArrayEquals(arrayOf(0L, 14L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(6L, 16L), stream.bufferEndOffsets)
            assertCacheMatchesRealData(14L to 16L, stream)

            // read next buffer before cache but full again
            stream.seek(12) // 12..18 but actually we trim right part: 12..16
            stream.read()
            Assert.assertArrayEquals(arrayOf(0L, 12L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(6L, 16L), stream.bufferEndOffsets)
            assertCacheMatchesRealData(12L to 16L, stream)
        }
    }

    @Test
    fun fetchNewBufferTrimmedIntersectingCacheSingleBuffer() {
        //easier to test with off double buffer

        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 6, false) {
            override fun fetchNewBuffer(pos: Long, buffOffset: Int, size: Int) = super.fetchNewBuffer(
                    pos, buffOffset,
                    if (pos == 14L) 2 else size
            )
        }.use { stream ->
            // fill whole buffer
            stream.seek(0) // 0..6
            stream.read()

            // validate
            Assert.assertArrayEquals(arrayOf(0L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(6L, 0L), stream.bufferEndOffsets)

            // read next buffer but less than buffer size
            stream.seek(14) // 14..16
            stream.read()
            Assert.assertArrayEquals(arrayOf(14L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(16L, 0L), stream.bufferEndOffsets)
            assertCacheMatchesRealData(14L to 16L, stream)

            // read next buffer before cache but full again
            stream.seek(12) // 12..18 but actually we trim right part: 12..16
            stream.read()
            Assert.assertArrayEquals(arrayOf(12L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(16L, 0L), stream.bufferEndOffsets)
            assertCacheMatchesRealData(12L to 16L, stream)
        }
    }

    @Test
    fun fetchNewBufferTrimmedIntersectingCacheWithGap() {
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 6) {
            override fun fetchNewBuffer(pos: Long, buffOffset: Int, size: Int) = super.fetchNewBuffer(
                    pos, buffOffset,
                    when (pos) {
                        12L -> 1
                        14L -> 2
                        else -> size
                    }
            )
        }.use { stream ->
            // fill whole buffer
            stream.seek(7) // 7..13
            stream.read()
            // fill 2nd buffer in case of double buffer
            stream.seek(0) // 0..6
            stream.read()
            // validate
            Assert.assertArrayEquals(arrayOf(0L, 7L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(6L, 13L), stream.bufferEndOffsets)

            // read next buffer but less than buffer size
            stream.seek(14) // 14..16
            stream.read()
            Assert.assertArrayEquals(arrayOf(0L, 14L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(6L, 16L), stream.bufferEndOffsets)
            assertCacheMatchesRealData(14L to 16L, stream)

            // read next buffer before cache but full again
            stream.seek(12) // 12..18 but actually we trim right part: 12..16
            stream.read()
            Assert.assertArrayEquals(arrayOf(0L, 12L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(6L, 13L), stream.bufferEndOffsets)
            assertCacheMatchesRealData(12L to 13L, stream)
        }
    }

    @Test
    fun fetchNewBufferTrimmedIntersectingCacheWithGapSingleBuffer() {
        //easier to test with off double buffer
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 6, false) {
            override fun fetchNewBuffer(pos: Long, buffOffset: Int, size: Int) = super.fetchNewBuffer(
                    pos, buffOffset,
                    when (pos) {
                        12L -> 1
                        14L -> 2
                        else -> size
                    }
            )
        }.use { stream ->
            // fill whole buffer
            stream.seek(0) // 0..6
            stream.read()
            // validate
            Assert.assertArrayEquals(arrayOf(0L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(6L, 0L), stream.bufferEndOffsets)

            // read next buffer but less than buffer size
            stream.seek(14) // 14..16
            stream.read()
            Assert.assertArrayEquals(arrayOf(14L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(16L, 0L), stream.bufferEndOffsets)
            assertCacheMatchesRealData(14L to 16L, stream)

            // read next buffer before cache but full again
            stream.seek(12) // 12..18 but actually we trim right part: 12..16
            stream.read()
            Assert.assertArrayEquals(arrayOf(12L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(13L, 0L), stream.bufferEndOffsets)
            assertCacheMatchesRealData(12L to 13L, stream)
        }
    }

    @Test
    fun exceptionInFetchBuffer() {
        object : BetterSeekableBufferedStream(ByteArraySeekableStream(TEST_DATA), 6, false) {
            override fun fetchNewBuffer(pos: Long, buffOffset: Int, size: Int) = when (pos) {
                14L -> error("Some error")
                else -> super.fetchNewBuffer(pos, buffOffset, size)
            }
        }.use { stream ->
            stream.seek(16) // 16..22
            stream.read()
            Assert.assertArrayEquals(arrayOf(16L, 0L), stream.bufferStartOffsets)
            Assert.assertArrayEquals(arrayOf(22L, 0L), stream.bufferEndOffsets)

            stream.seek(14) // 14..20

            var exceptionMsg = "<n/a>"
            try {
                stream.read()
            } catch (e: IllegalStateException) {
                exceptionMsg = e.message!!
            }
            assertEquals("Some error", exceptionMsg)
            assertCacheMatchesRealData(0L to 0L, stream)

        }
    }

    companion object {
        val TEST_DATA = byteArrayOf(1, 3, 4, 7, 9, 12, Byte.MAX_VALUE, 34, -1, Byte.MIN_VALUE,
                100, 101, 102, 103, 104, -10, -11, -12, -13, -14, -15, -16, -17, -18)

        fun assertCacheMatchesRealData(
                expectedBufferStartEnd: Pair<Long, Long>,
                stream: BetterSeekableBufferedStream,
                bufferOffset: Int = 0
        ) {
            assertEquals(expectedBufferStartEnd, stream.bufferStartOffset to stream.bufferEndOffset)
            assertBufferMatchesRealData(
                    stream.bufferStartOffset.toInt() to stream.bufferEndOffset.toInt(),
                    stream.buffer!!, bufferOffset)
        }

        fun assertBufferMatchesRealData(
                expectedBufferStartEnd: Pair<Int, Int>,
                buffer: ByteArray,
                bufferOffset: Int = 0
        ) {
            val fromIndex = expectedBufferStartEnd.first.toInt()
            val toIndex = expectedBufferStartEnd.second.toInt()
            assertEquals(
                    TEST_DATA.toList().subList(fromIndex, toIndex),
                    buffer.toList().subList(bufferOffset + 0, bufferOffset + toIndex - fromIndex)
            )
        }
    }
}

