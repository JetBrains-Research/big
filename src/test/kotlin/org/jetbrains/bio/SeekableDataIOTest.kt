package org.jetbrains.bio

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.test.assertEquals

@RunWith(Parameterized::class)
class SeekableDataIOTest(private val order: ByteOrder) {
    @Test fun testWriteReadIntegral() = withTempFileRandomized() { path, r ->
        val b = r.nextByte()
        val s = r.nextInt(Short.MAX_VALUE.toInt())
        val i = r.nextInt()
        val l = r.nextLong()
        CountingDataOutput.of(path, order).use {
            it.writeByte(b.toInt())
            it.writeShort(s)
            it.writeInt(i)
            it.writeLong(l)
        }
        BigByteBuffer.of(path, order).let {
            assertEquals(b, it.get())
            assertEquals(s.toShort(), it.getShort())
            assertEquals(i, it.getInt())
            assertEquals(l, it.getLong())
        }
    }

    @Test fun testWriteReadFloating() = withTempFileRandomized() { path, r ->
        val f = r.nextFloat()
        val d = r.nextDouble()
        CountingDataOutput.of(path, order).use {
            it.writeFloat(f)
            it.writeDouble(d)
        }
        BigByteBuffer.of(path, order).let {
            assertEquals(f, it.getFloat())
            assertEquals(d, it.getDouble())
        }
    }

    @Test fun testWriteReadChars() = withTempFileRandomized() { path, r ->
        val s = (0..r.nextInt(100)).map { (r.nextInt(64) + 32).toString() }.joinToString("")
        CountingDataOutput.of(path, order).use {
            it.writeCString(s)
            it.writeCString(s, s.length + 8)
            it.skipBytes(16)
        }
        BigByteBuffer.of(path, order).let {
            assertEquals(s, it.getCString())
            var b = ByteArray(s.length + 8)
            it.get(b)
            assertEquals(s, String(b).trimZeros())

            for (i in 0 until 16) {
                assertEquals(0.toByte(), it.get())
            }
        }
    }

    @Test fun testCompression() = withTempFileRandomized { path, r ->
        val b = (0..r.nextInt(100)).map { r.nextByte() }.toByteArray()
        CountingDataOutput.of(path, order).use {
            it.with(compressed = true) {
                b.forEach { writeByte(it.toInt()) }
            }
        }

        BigByteBuffer.of(path, order).let {
            it.with(0L, Files.size(path), compressed = true) {
                for (i in 0 until b.size) {
                    assertEquals(b[i], get())
                }
            }
        }
    }

    private inline fun withTempFileRandomized(block: (Path, Random) -> Unit) {
        withTempFile(order.toString(), ".bb") { path ->
            val attempts = RANDOM.nextInt(100) + 1
            for (i in 0 until attempts) {
                block(path, RANDOM)
            }
        }
    }

    private fun Random.nextByte(): Byte {
        val b = nextInt(Byte.MAX_VALUE - Byte.MIN_VALUE)
        return (b + Byte.MIN_VALUE).toByte()
    }

    companion object {
        val RANDOM: Random = Random()  // private causes compiler crash.

        @Parameters(name = "{0}")
        @JvmStatic fun data(): Iterable<ByteOrder> {
            return listOf(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)
        }
    }
}