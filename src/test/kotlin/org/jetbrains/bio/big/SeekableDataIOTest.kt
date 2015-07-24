package org.jetbrains.bio.big

import com.google.common.math.IntMath
import org.junit.Assert.assertArrayEquals
import org.junit.Test
import java.nio.ByteOrder
import java.nio.file.Files
import java.util.Random
import kotlin.test.assertEquals

public class SeekableDataOutputTest {
    Test fun testBigEndian() {
        for (i in 0..NUM_ATTEMPTS - 1) {
            testWriteRead(ByteOrder.BIG_ENDIAN)
        }
    }

    Test fun testLittleEndian() {
        for (i in 0..NUM_ATTEMPTS - 1) {
            testWriteRead(ByteOrder.LITTLE_ENDIAN)
        }
    }

    private fun testWriteRead(byteOrder: ByteOrder) {
        val shortValue = RANDOM.nextInt().toShort()
        val unsignedShortValue = RANDOM.nextInt(IntMath.pow(2, 16))
        val intValue = RANDOM.nextInt()
        val longValue = RANDOM.nextLong()
        val floatValue = RANDOM.nextFloat()
        val doubleValue = RANDOM.nextDouble()
        val charsArrayValue = "I'm a char array"
        val severalSingleChar = 'a'
        val severalSingleCharCount = 3

        val chromosomeName = "chrom1"
        val keySize = 12

        val path = Files.createTempFile(byteOrder.toString(), ".bb")
        try {
            SeekableDataOutput.of(path, byteOrder).use { output ->
                with(output) {
                    writeShort(shortValue.toInt())
                    writeUnsignedShort(unsignedShortValue)
                    writeInt(intValue)
                    writeLong(longValue)
                    writeFloat(floatValue)
                    writeDouble(doubleValue)
                    writeBytes(charsArrayValue)
                    skipBytes(severalSingleChar.toInt(), severalSingleCharCount)
                    writeBytes(chromosomeName, keySize)
                }
            }

            SeekableDataInput.of(path, byteOrder).use { input ->
                with(input) {
                    assertEquals(shortValue, readShort())
                    assertEquals(unsignedShortValue, readUnsignedShort())
                    assertEquals(intValue, readInt())
                    assertEquals(longValue, readLong())
                    assertEquals(floatValue, readFloat())
                    assertEquals(doubleValue, readDouble())
                }

                val charsArrayValueReader = ByteArray(charsArrayValue.length())
                input.readFully(charsArrayValueReader)
                assertArrayEquals(charsArrayValue.toByteArray(), charsArrayValueReader)

                val charSingle = ByteArray(severalSingleCharCount)
                input.readFully(charSingle)
                for (b in charSingle) {
                    assertEquals(severalSingleChar.toLong(), b.toLong())
                }

                val chromosomeNameExt = ByteArray(keySize)
                input.readFully(chromosomeNameExt)
                for (i in 0..keySize - 1) {
                    if (i < chromosomeName.length()) {
                        assertEquals(chromosomeName[i].toInt(), chromosomeNameExt[i].toInt())
                    } else {
                        assertEquals(0, chromosomeNameExt[i].toInt())
                    }
                }
            }
        } finally {
            Files.deleteIfExists(path)
        }
    }

    companion object {
        private val RANDOM = Random()
        private val NUM_ATTEMPTS = 1000
    }
}
