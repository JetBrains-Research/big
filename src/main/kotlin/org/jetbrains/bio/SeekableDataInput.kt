package org.jetbrains.bio

import java.io.Closeable
import java.io.DataInput
import java.io.EOFException
import java.io.IOException

/**
 * `java.io.DataInput` interface extension with `seek` and additional read methods
 */
interface SeekableDataInput : DataInput, Closeable {
    fun position(): Long
    fun length(): Long
    fun seek(position: Long)

    fun read(buffer: ByteArray, offset: Int, length: Int): Int

    /**
     * Read an array of ints
     *
     * @param pa    read into this array
     * @param start starting at pa[start]
     * @param n     read this many elements
     * @throws EOFException if this stream reaches the end before reading whole array.
     * @throws IOException if an I/O error occurs.
     */
    @Throws(IOException::class)
    fun readInt(pa: IntArray, start: Int, n: Int) {
        for (i in 0 until n) {
            pa[start + i] = readInt()
        }
    }

    /**
     * Read an array of longs
     *
     * @param pa    read into this array
     * @param start starting at pa[start]
     * @param n     read this many elements
     * @throws EOFException if this stream reaches the end before reading whole array.
     * @throws IOException if an I/O error occurs.
     */
    @Throws(IOException::class)
    fun readLong(pa: LongArray, start: Int, n: Int) {
        for (i in 0 until n) {
            pa[start + i] = readLong()
        }
    }

    /**
     * Read an array of floats
     *
     * @param pa    read into this array
     * @param start starting at pa[start]
     * @param n     read this many elements
     * @throws EOFException if this stream reaches the end before reading whole array.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    fun readFloat(pa: FloatArray, start: Int, n: Int) {
        for (i in 0 until n) {
            pa[start + i] = java.lang.Float.intBitsToFloat(readInt())
        }
    }

    /**
     * Read an array of doubles
     *
     * @param pa    read into this array
     * @param start starting at pa[start]
     * @param n     read this many elements
     * @throws EOFException if this stream reaches the end before reading whole array.
     * @throws IOException  if an I/O error occurs.
     * @see java.lang.Double.longBitsToDouble
     */
    @Throws(IOException::class)
    fun readDouble(pa: DoubleArray, start: Int, n: Int) {
        for (i in 0 until n) {
            pa[start + i] = java.lang.Double.longBitsToDouble(readLong())
        }
    }
}