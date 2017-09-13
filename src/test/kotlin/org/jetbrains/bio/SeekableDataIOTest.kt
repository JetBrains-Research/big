package org.jetbrains.bio

import com.indeed.util.mmap.MMapBuffer
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.test.assertEquals

@RunWith(Parameterized::class)
class SeekableDataIOTest(private val order: ByteOrder,
                         private val compression: CompressionType) {
    @Test fun testWriteReadIntegral() = withTempFileRandomized() { path, r ->
        val b = r.nextInt().toByte()
        val s = r.nextInt(Short.MAX_VALUE.toInt())
        val i = r.nextInt()
        val l = r.nextLong()
        OrderedDataOutput(path, order).use {
            it.writeByte(b.toInt())
            it.writeShort(s)
            it.writeInt(i)
            it.writeLong(l)
        }
        MMBRomBuffer(MMapBuffer(path, FileChannel.MapMode.READ_ONLY, order)).use {
            assertEquals(b, it.readByte())
            assertEquals(s.toShort(), it.readShort())
            assertEquals(i, it.readInt())
            assertEquals(l, it.readLong())
        }
    }

    @Test fun testWriteReadFloating() = withTempFileRandomized() { path, r ->
        val f = r.nextFloat()
        val d = r.nextDouble()
        OrderedDataOutput(path, order).use {
            it.writeFloat(f)
            it.writeDouble(d)
        }
        MMBRomBuffer(MMapBuffer(path, FileChannel.MapMode.READ_ONLY, order)).use {
            assertEquals(f, it.readFloat())
            assertEquals(d, it.readDouble())
        }
    }

    @Test fun testWriteReadChars() = withTempFileRandomized() { path, r ->
        val s = (0..r.nextInt(100)).map { (r.nextInt(64) + 32).toString() }.joinToString("")
        OrderedDataOutput(path, order).use {
            it.writeString(s)
            it.writeByte(0)
            it.writeString(s, s.length + 8)
            it.skipBytes(16)
        }
        MMBRomBuffer(MMapBuffer(path, FileChannel.MapMode.READ_ONLY, order)).use {
            assertEquals(s, it.readCString())
            val b = it.readBytes(s.length + 8)
            assertEquals(s, String(b).trimEnd { it == '\u0000' })

            for (i in 0 until 16) {
                assertEquals(0.toByte(), it.readByte())
            }
        }
    }

    @Test fun testCompression() = withTempFileRandomized { path, r ->
        val values = (0..r.nextInt(4096)).map { r.nextInt() }.toIntArray()
        OrderedDataOutput(path, order).use {
            it.with(compression) {
                values.forEach { writeInt(it) }
            }
        }

        MMBRomBuffer(MMapBuffer(path, FileChannel.MapMode.READ_ONLY, order)).use {
            it.with(0, Files.size(path), compression) {
                for (i in 0 until values.size) {
                    assertEquals(values[i], readInt())
                }
            }
        }
    }

    @Test fun testPositionCopyingDuringDuplication() = withTempFile(order.toString(), ".bb") { path ->
        val values = arrayListOf(0, 10)
        OrderedDataOutput(path, order).use { orderedDataOutput ->
            values.forEach { orderedDataOutput.writeInt(it) }
        }

        MMBRomBuffer(MMapBuffer(path, FileChannel.MapMode.READ_ONLY, order)).use { buffer ->
            buffer.readInt()
            val second = buffer.duplicate()

            assertEquals(values[1], second.readInt())
        }
    }

    private inline fun withTempFileRandomized(block: (Path, Random) -> Unit) {
        val attempts = RANDOM.nextInt(100) + 1
        for (i in 0 until attempts) {
            withTempFile(order.toString(), ".bb") { block(it, RANDOM) }
        }
    }

    companion object {
        private val RANDOM = Random()

        @Parameters(name = "{0}:{1}")
        @JvmStatic fun data(): Iterable<Array<Any>> {
            return listOf(arrayOf(ByteOrder.BIG_ENDIAN, CompressionType.NO_COMPRESSION),
                          arrayOf(ByteOrder.BIG_ENDIAN, CompressionType.DEFLATE),
                          arrayOf(ByteOrder.BIG_ENDIAN, CompressionType.SNAPPY),
                          arrayOf(ByteOrder.LITTLE_ENDIAN, CompressionType.NO_COMPRESSION),
                          arrayOf(ByteOrder.LITTLE_ENDIAN, CompressionType.DEFLATE),
                          arrayOf(ByteOrder.LITTLE_ENDIAN, CompressionType.SNAPPY))
        }
    }
}
