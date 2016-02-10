package org.jetbrains.bio

import org.junit.Assert.assertArrayEquals
import org.junit.Test

class FastByteArrayOutputStreamTest {
    @Test fun writeByte() {
        val output = FastByteArrayOutputStream()
        output.write(42)
        assertArrayEquals(byteArrayOf(42), output.toByteArray())
    }

    @Test fun writeEmptyByteArray() {
        val output = FastByteArrayOutputStream()
        output.write(byteArrayOf())
        assertArrayEquals(byteArrayOf(), output.toByteArray())
    }

    @Test fun writeByteArray() {
        val output = FastByteArrayOutputStream()
        output.write(byteArrayOf(1, 2, 3, 4, 5, 6, 7, 8, 9), 1, 4)
        assertArrayEquals(byteArrayOf(2, 3, 4, 5), output.toByteArray())
    }
}