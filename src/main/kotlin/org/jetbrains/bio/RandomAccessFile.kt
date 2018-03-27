/*
 * Copyright 1998-2014 University Corporation for Atmospheric Research/Unidata
 *
 *   Portions of this software were developed by the Unidata Program at the
 *   University Corporation for Atmospheric Research.
 *
 *   Access and use of this software shall impose the following obligations
 *   and understandings on the user. The user is granted the right, without
 *   any fee or cost, to use, copy, modify, alter, enhance and distribute
 *   this software, and any derivative works thereof, and its supporting
 *   documentation for any purpose whatsoever, provided that this entire
 *   notice appears in all copies of the software, derivative works and
 *   supporting documentation.  Further, UCAR requests that the user credit
 *   UCAR/Unidata in any publications that result from the use of this
 *   software or in any product that includes this software. The names UCAR
 *   and/or Unidata, however, may not be used in any advertising or publicity
 *   to endorse or promote any products or commercial entity unless specific
 *   written permission is obtained from UCAR/Unidata. The user also
 *   understands that UCAR/Unidata is not obligated to provide the user with
 *   any support, consulting, training or assistance of any kind with regard
 *   to the use, operation and performance of this software nor to provide
 *   the user with any updates, revisions, new versions or "bug fixes."
 *
 *   THIS SOFTWARE IS PROVIDED BY UCAR/UNIDATA "AS IS" AND ANY EXPRESS OR
 *   IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *   WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *   DISCLAIMED. IN NO EVENT SHALL UCAR/UNIDATA BE LIABLE FOR ANY SPECIAL,
 *   INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING
 *   FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
 *   NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION
 *   WITH THE ACCESS, USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package org.jetbrains.bio

import org.apache.log4j.LogManager
import java.io.*
import java.nio.ByteOrder
import java.nio.channels.WritableByteChannel
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.HashSet


/**
 * Limited version of RandomAccessFile: https://www.unidata.ucar.edu, "netcdf-java" project
 * (https://github.com/Unidata/thredds/blob/master/cdm/src/main/java/ucar/unidata/io/RandomAccessFile.java)
 *
 * ==============================================================================
 * A buffered drop-in replacement for java.io.RandomAccessFile.
 * Instances of this class realise substantial speed increases over
 * java.io.RandomAccessFile through the use of buffering. This is a
 * subclass of Object, as it was not possible to subclass
 * java.io.RandomAccessFile because many of the methods are
 * final. However, if it is necessary to use RandomAccessFile and
 * java.io.RandomAccessFile interchangeably, both classes implement the
 * DataInput and DataOutput interfaces.
 *
 *
 *
 *  By Russ Rew, based on
 * BufferedRandomAccessFile by Alex McManus, based on Sun's source code
 * for java.io.RandomAccessFile.  For Alex McManus version from which
 * this derives, see his [
 * Freeware Java Classes](http://www.aber.ac.uk/~agm/Java.html).
 *
 *
 * Must be thread confined - that is, can only be used by a single thread at a time..
 *
 * @author Alex McManus
 * @author Russ Rew
 * @author john caron
 * @author Roman.Chernyatchik
 * @see java.io.DataInput
 * @see java.io.RandomAccessFile
 *
 * 
 * @param location File location or name
 */
open class RandomAccessFile(val location: String, bufferSize: Int = defaultBufferSize)
    : DataInput, Closeable {

    /**
     * The underlying java.io.RandomAccessFile
     */
    var randomAccessFile: java.io.RandomAccessFile?
    var fileChannel: java.nio.channels.FileChannel? = null

    /**
     * Current position in the file, where the next read or
     * write will occur.
     */
    var filePointer: Long = 0
        protected set

    /**
     * The buffer used for reading the data.
     */
    protected lateinit var buffer: ByteArray

    /**
     * The offset in bytes of the start of the buffer, from the start of the file.
     */
    protected var bufferStart: Long = 0

    /**
     * The offset in bytes of the end of the data in the buffer, from
     * the start of the file. This can be calculated from
     * `bufferStart + dataSize`, but it is cached to speed
     * up the read( ) method.
     */
    protected var dataEnd: Long = 0

    /**
     * The size of the data stored in the buffer, in bytes. This may be
     * less than the size of the buffer.
     */
    protected var dataSize: Int = 0

    /**
     * True if we are at the end of the file.
     */
    var endOfFile: Boolean = false
        protected set

    /**
     * The current endian (big or little) mode of the file.
     */
    protected var bigEndian: Boolean = false

    /**
     * Internal buffer size in bytes. If writing, call flush() first.
     */
    var bufferSize: Int
        get() = buffer.size
        set(bufferSize) {
            bufferStart = 0
            dataEnd = 0
            dataSize = 0
            filePointer = 0
            buffer = ByteArray(bufferSize)
            endOfFile = false
        }


    init {
        this.bufferSize = if (bufferSize < 0) defaultBufferSize else bufferSize
        if (debugLeaks) {
            _allFiles.add(location)
        }

        try {
            randomAccessFile = java.io.RandomAccessFile(location, "r")
        } catch (ex: IOException) {
            if (ex.message == "Too many open files") {
                LOG.error(ex)
                try {
                    Thread.sleep(100)
                } catch (e: InterruptedException) {
                    LOG.warn(ex)
                }

                // Windows having trouble keeping up ??
                randomAccessFile = java.io.RandomAccessFile(location, "r")
            } else {
                throw ex
            }
        }

        if (debugLeaks) {
            _openedFiles.add(location)
            maxOpenedFilesCounter.set(Math.max(openedFiles.size, maxOpenedFilesCounter.get()))
            totalOpenedFilesCounter.getAndIncrement()
            if (logOpen) {
                LOG.debug("  open " + location)
            }
            if (openedFiles.size > 1000) {
                LOG.warn("RandomAccessFile debugLeaks")
            }
        }
    }

    /**
     * Close the file, and release any associated system resources.
     *
     * @throws IOException if an I/O error occurrs.
     */
    @Synchronized
    @Throws(IOException::class)
    override fun close() {
        if (debugLeaks) {
            _openedFiles.remove(location)
            if (logOpen) LOG.debug("  close " + location)
        }

        if (randomAccessFile == null) {
            return
        }

        // Close the underlying file object.
        randomAccessFile!!.close()
        randomAccessFile = null  // help the gc
    }

    /**
     * Set the position in the file for the next read or write.
     *
     * @param pos the offset (in bytes) from the start of the file.
     * @throws IOException if an I/O error occurrs.
     */
    @Throws(IOException::class)
    fun seek(pos: Long) {
        if (pos < 0)
            throw java.io.IOException("Negative seek offset")

        // If the seek is into the buffer, just update the file pointer.
        if (pos in bufferStart..(dataEnd - 1)) {
            filePointer = pos
            return
        }

        // need new buffer, starting at pos
        readBuffer(pos)
    }

    @Throws(IOException::class)
    protected fun readBuffer(pos: Long) {
        bufferStart = pos
        filePointer = pos

        dataSize = read_(pos, buffer, 0, buffer.size)

        if (dataSize <= 0) {
            dataSize = 0
            endOfFile = true
        } else {
            endOfFile = false
        }

        // Cache the position of the buffer end.
        dataEnd = bufferStart + dataSize
    }

    /**
     * Get the length of the file. The data in the buffer is taken into account.
     *
     * @return the length of the file in bytes.
     * @throws IOException if an I/O error occurrs.
     */
    @Throws(IOException::class)
    fun length() =
            // GRIB has closed the data raf
            if (randomAccessFile == null) -1L else randomAccessFile!!.length()


    /**
     * Change the current endian mode. Subsequent reads of short, int, float, double, long, char will
     * use this.
     * Default values is BIG_ENDIAN.
     */
    fun order(byteOrder: ByteOrder) {
        this.bigEndian = byteOrder == ByteOrder.BIG_ENDIAN
    }


    //////////////////////////////////////////////////////////////////////////////////////////////
    // Read primitives.
    //

    /**
     * Read a byte of data from the file, blocking until data is
     * available.
     *
     * @return the next byte of data, or -1 if the end of the file is
     * reached.
     * @throws IOException if an I/O error occurrs.
     */
    @Throws(IOException::class)
    fun read(): Int {

        return when {
        // If the file position is within the data, return the byte...
            filePointer < dataEnd -> {
                val pos = (filePointer - bufferStart).toInt()
                filePointer++
                (buffer[pos].toInt() and 0xff)


            }
        // ...or should we indicate EOF...
            endOfFile -> -1

        // ...or seek to fill the buffer, and try again.
            else -> {
                seek(filePointer)
                read()
            }
        }
    }


    /**
     * Read up to `len` bytes into an array, at a specified
     * offset. This will block until at least one byte has been read.
     *
     * @param b   the byte array to receive the bytes.
     * @param off the offset in the array where copying will start.
     * @param len the number of bytes to copy.
     * @return the actual number of bytes read, or -1 if there is not
     * more data due to the end of the file being reached.
     * @throws IOException if an I/O error occurrs.
     */
    @Throws(IOException::class)
    protected fun readBytes(b: ByteArray, off: Int, len: Int): Int {

        // Check for end of file.
        if (endOfFile) {
            return -1
        }

        // See how many bytes are available in the buffer - if none,
        // seek to the file position to update the buffer and try again.
        val bytesAvailable = (dataEnd - filePointer).toInt()
        if (bytesAvailable < 1) {
            seek(filePointer)
            return readBytes(b, off, len)
        }

        // Copy as much as we can.
        val copyLength = if (bytesAvailable >= len)
            len
        else
            bytesAvailable
        System.arraycopy(buffer, (filePointer - bufferStart).toInt(), b, off, copyLength)
        filePointer += copyLength.toLong()

        // If there is more to copy...
        if (copyLength < len) {
            var extraCopy = len - copyLength

            // If the amount remaining is more than a buffer's length, read it
            // directly from the file.
            if (extraCopy > buffer.size) {
                extraCopy = read_(filePointer, b, off + copyLength, len - copyLength)

                // ...or read a new buffer full, and copy as much as possible...
            } else {
                seek(filePointer)
                if (!endOfFile) {
                    extraCopy = if (extraCopy > dataSize)
                        dataSize
                    else
                        extraCopy
                    System.arraycopy(buffer, 0, b, off + copyLength, extraCopy)
                } else {
                    extraCopy = -1
                }
            }

            // If we did manage to copy any more, update the file position and
            // return the amount copied.
            if (extraCopy > 0) {
                filePointer += extraCopy.toLong()
                return copyLength + extraCopy
            }
        }

        // Return the amount copied.
        return copyLength
    }


    /**
     * Read `nbytes` bytes, at the specified file offset, send to a WritableByteChannel.
     * This will block until all bytes are read.
     * This uses the underlying file channel directly, bypassing all user buffers.
     *
     * @param dest   write to this WritableByteChannel.
     * @param offset the offset in the file where copying will start.
     * @param nbytes the number of bytes to read.
     * @return the actual number of bytes read and transfered
     * @throws IOException if an I/O error occurs.
     */
    @Throws(IOException::class)
    fun readToByteChannel(dest: WritableByteChannel, offset: Long, nbytes: Long): Long {
        var offset = offset

        if (fileChannel == null)
            fileChannel = randomAccessFile!!.channel

        var need = nbytes
        while (need > 0) {
            val count = fileChannel!!.transferTo(offset, need, dest)
            //if (count == 0) break;  // LOOK not sure what the EOF condition is
            need -= count
            offset += count
        }
        return nbytes - need
    }

    /**
     * Read up to `len` bytes into an array, at a specified
     * offset. This will block until at least one byte has been read.
     *
     * @param b   the byte array to receive the bytes.
     * @param off the offset in the array where copying will start.
     * @param len the number of bytes to copy.
     * @return the actual number of bytes read, or -1 if there is not
     * more data due to the end of the file being reached.
     * @throws IOException if an I/O error occurrs.
     */
    @Throws(IOException::class)
    fun read(b: ByteArray, off: Int, len: Int) = readBytes(b, off, len)

    /**
     * Read up to `b.length( )` bytes into an array. This
     * will block until at least one byte has been read.
     *
     * @param b the byte array to receive the bytes.
     * @return the actual number of bytes read, or -1 if there is not
     * more data due to the end of the file being reached.
     * @throws IOException if an I/O error occurrs.
     */
    @Throws(IOException::class)
    fun read(b: ByteArray) = readBytes(b, 0, b.size)

    /**
     * Read fully count number of bytes
     *
     * @param count how many bytes tp read
     * @return a byte array of length count, fully read in
     * @throws IOException if an I/O error occurrs.
     */
    @Throws(IOException::class)
    fun readBytes(count: Int): ByteArray {
        val b = ByteArray(count)
        readFully(b)
        return b
    }


    /**
     * Reads `b.length` bytes from this file into the byte
     * array. This method reads repeatedly from the file until all the
     * bytes are read. This method blocks until all the bytes are read,
     * the end of the stream is detected, or an exception is thrown.
     *
     * @param b the buffer into which the data is read.
     * @throws EOFException if this file reaches the end before reading
     * all the bytes.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readFully(b: ByteArray) {
        readFully(b, 0, b.size)
    }

    /**
     * Reads exactly `len` bytes from this file into the byte
     * array. This method reads repeatedly from the file until all the
     * bytes are read. This method blocks until all the bytes are read,
     * the end of the stream is detected, or an exception is thrown.
     *
     * @param b   the buffer into which the data is read.
     * @param off the start offset of the data.
     * @param len the number of bytes to read.
     * @throws EOFException if this file reaches the end before reading
     * all the bytes.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readFully(b: ByteArray, off: Int, len: Int) {
        var n = 0
        while (n < len) {
            val count = this.read(b, off + n, len - n)
            if (count < 0) {
                throw EOFException("Reading " + location + " at " + filePointer + " file length = " + length())
            }
            n += count
        }
    }


    /**
     * Reads a `boolean` from this file. This method reads a
     * single byte from the file. A value of `0` represents
     * `false`. Any other value represents `true`.
     * This method blocks until the byte is read, the end of the stream
     * is detected, or an exception is thrown.
     *
     * @return the `boolean` value read.
     * @throws EOFException if this file has reached the end.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readBoolean(): Boolean {
        val ch = read()
        if (ch < 0) {
            throw EOFException()
        }
        return ch != 0
    }


    /**
     * Reads a signed 8-bit value from this file. This method reads a
     * byte from the file. If the byte read is `b`, where
     * `0&nbsp;<=&nbsp;b&nbsp;<=&nbsp;255`,
     * then the result is:
     * `
     * (byte)(b)
    ` *
     *
     *
     * This method blocks until the byte is read, the end of the stream
     * is detected, or an exception is thrown.
     *
     * @return the next byte of this file as a signed 8-bit
     * `byte`.
     * @throws EOFException if this file has reached the end.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readByte(): Byte {
        val ch = read()
        if (ch < 0) {
            throw EOFException()
        }
        return ch.toByte()
    }


    /**
     * Reads an unsigned 8-bit number from this file. This method reads
     * a byte from this file and returns that byte.
     *
     *
     * This method blocks until the byte is read, the end of the stream
     * is detected, or an exception is thrown.
     *
     * @return the next byte of this file, interpreted as an unsigned
     * 8-bit number.
     * @throws EOFException if this file has reached the end.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readUnsignedByte(): Int {
        val ch = this.read()
        if (ch < 0) {
            throw EOFException()
        }
        return ch
    }


    /**
     * Reads a signed 16-bit number from this file. The method reads 2
     * bytes from this file. If the two bytes read, in order, are
     * `b1` and `b2`, where each of the two values is
     * between `0` and `255`, inclusive, then the
     * result is equal to:
     * `
     * (short)((b1 << 8) | b2)
    ` *
     *
     *
     * This method blocks until the two bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return the next two bytes of this file, interpreted as a signed
     * 16-bit number.
     * @throws EOFException if this file reaches the end before reading
     * two bytes.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readShort(): Short {
        val ch1 = this.read()
        val ch2 = this.read()
        if (ch1 or ch2 < 0) {
            throw EOFException()
        }
        return if (bigEndian) {
            ((ch1 shl 8) + ch2).toShort()
        } else {
            ((ch2 shl 8) + ch1).toShort()
        }
    }


    /**
     * Read an array of shorts
     *
     * @param pa    read into this array
     * @param start starting at pa[start]
     * @param n     read this many elements
     * @throws IOException on read error
     */
    @Throws(IOException::class)
    fun readShort(pa: ShortArray, start: Int, n: Int) {
        for (i in 0 until n) {
            pa[start + i] = readShort()
        }
    }

    /**
     * Reads an unsigned 16-bit number from this file. This method reads
     * two bytes from the file. If the bytes read, in order, are
     * `b1` and `b2`, where
     * `0&nbsp;<=&nbsp;b1, b2&nbsp;<=&nbsp;255`,
     * then the result is equal to:
     * `
     * (b1 << 8) | b2
    ` *
     *
     *
     * This method blocks until the two bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return the next two bytes of this file, interpreted as an unsigned
     * 16-bit integer.
     * @throws EOFException if this file reaches the end before reading
     * two bytes.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readUnsignedShort(): Int {
        val ch1 = this.read()
        val ch2 = this.read()
        if (ch1 or ch2 < 0) {
            throw EOFException()
        }
        return if (bigEndian) {
            (ch1 shl 8) + ch2
        } else {
            (ch2 shl 8) + ch1
        }
    }


    /**
     * Reads a Unicode character from this file. This method reads two
     * bytes from the file. If the bytes read, in order, are
     * `b1` and `b2`, where
     * `0&nbsp;<=&nbsp;b1,&nbsp;b2&nbsp;<=&nbsp;255`,
     * then the result is equal to:
     * `
     * (char)((b1 << 8) | b2)
    ` *
     *
     *
     * This method blocks until the two bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return the next two bytes of this file as a Unicode character.
     * @throws EOFException if this file reaches the end before reading
     * two bytes.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readChar(): Char {
        val ch1 = this.read()
        val ch2 = this.read()
        if (ch1 or ch2 < 0) {
            throw EOFException()
        }
        return if (bigEndian) {
            ((ch1 shl 8) + ch2).toChar()
        } else {
            ((ch2 shl 8) + ch1).toChar()
        }
    }

    /**
     * Reads a signed 32-bit integer from this file. This method reads 4
     * bytes from the file. If the bytes read, in order, are `b1`,
     * `b2`, `b3`, and `b4`, where
     * `0&nbsp;<=&nbsp;b1, b2, b3, b4&nbsp;<=&nbsp;255`,
     * then the result is equal to:
     * `
     * (b1 << 24) | (b2 << 16) + (b3 << 8) + b4
    ` *
     *
     *
     * This method blocks until the four bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return the next four bytes of this file, interpreted as an
     * `int`.
     * @throws EOFException if this file reaches the end before reading
     * four bytes.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readInt(): Int {
        val ch1 = this.read()
        val ch2 = this.read()
        val ch3 = this.read()
        val ch4 = this.read()
        if (ch1 or ch2 or ch3 or ch4 < 0) {
            throw EOFException()
        }

        return if (bigEndian) {
            (ch1 shl 24) + (ch2 shl 16) + (ch3 shl 8) + ch4
        } else {
            (ch4 shl 24) + (ch3 shl 16) + (ch2 shl 8) + ch1
        }
    }

    /**
     * Read an integer at the given position, bypassing all buffering.
     *
     * @param pos read a byte at this position
     * @return The int that was read
     * @throws IOException if an I/O error occurs.
     */
    @Throws(IOException::class)
    fun readIntUnbuffered(pos: Long): Int {
        val bb = ByteArray(4)
        read_(pos, bb, 0, 4)
        val ch1 = bb[0].toInt() and 0xff
        val ch2 = bb[1].toInt() and 0xff
        val ch3 = bb[2].toInt() and 0xff
        val ch4 = bb[3].toInt() and 0xff
        if (ch1 or ch2 or ch3 or ch4 < 0) {
            throw EOFException()
        }

        return if (bigEndian) {
            (ch1 shl 24) + (ch2 shl 16) + (ch3 shl 8) + ch4
        } else {
            (ch4 shl 24) + (ch3 shl 16) + (ch2 shl 8) + ch1
        }
    }

    /**
     * Read an array of ints
     *
     * @param pa    read into this array
     * @param start starting at pa[start]
     * @param n     read this many elements
     * @throws IOException on read error
     */
    @Throws(IOException::class)
    fun readInt(pa: IntArray, start: Int, n: Int) {
        for (i in 0 until n) {
            pa[start + i] = readInt()
        }
    }

    /**
     * Reads a signed 64-bit integer from this file. This method reads eight
     * bytes from the file. If the bytes read, in order, are
     * `b1`, `b2`, `b3`,
     * `b4`, `b5`, `b6`,
     * `b7`, and `b8,` where:
     * `
     * 0 <= b1, b2, b3, b4, b5, b6, b7, b8 <=255,
    ` *
     *
     *
     * then the result is equal to:
     *
     * <blockquote><pre>
     * ((long)b1 &lt;&lt; 56) + ((long)b2 &lt;&lt; 48)
     * + ((long)b3 &lt;&lt; 40) + ((long)b4 &lt;&lt; 32)
     * + ((long)b5 &lt;&lt; 24) + ((long)b6 &lt;&lt; 16)
     * + ((long)b7 &lt;&lt; 8) + b8
    </pre></blockquote> *
     *
     *
     * This method blocks until the eight bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return the next eight bytes of this file, interpreted as a
     * `long`.
     * @throws EOFException if this file reaches the end before reading
     * eight bytes.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readLong(): Long {
        return if (bigEndian) {
            (readInt().toLong() shl 32) + (readInt().toLong() and 0xFFFFFFFFL)  // tested ok
        } else {
            (readInt().toLong() and 0xFFFFFFFFL) + (readInt().toLong() shl 32) // not tested yet ??
        }
    }

    /**
     * Read an array of longs
     *
     * @param pa    read into this array
     * @param start starting at pa[start]
     * @param n     read this many elements
     * @throws IOException on read error
     */
    @Throws(IOException::class)
    fun readLong(pa: LongArray, start: Int, n: Int) {
        for (i in 0 until n) {
            pa[start + i] = readLong()
        }
    }


    /**
     * Reads a `float` from this file. This method reads an
     * `int` value as if by the `readInt` method
     * and then converts that `int` to a `float`
     * using the `intBitsToFloat` method in class
     * `Float`.
     *
     *
     * This method blocks until the four bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return the next four bytes of this file, interpreted as a
     * `float`.
     * @throws EOFException if this file reaches the end before reading
     * four bytes.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.RandomAccessFile.readInt
     * @see java.lang.Float.intBitsToFloat
     */
    @Throws(IOException::class)
    override fun readFloat(): Float {
        return java.lang.Float.intBitsToFloat(readInt())
    }

    /**
     * Read an array of floats
     *
     * @param pa    read into this array
     * @param start starting at pa[start]
     * @param n     read this many elements
     * @throws IOException on read error
     */
    @Throws(IOException::class)
    fun readFloat(pa: FloatArray, start: Int, n: Int) {
        for (i in 0 until n) {
            pa[start + i] = java.lang.Float.intBitsToFloat(readInt())
        }
    }


    /**
     * Reads a `double` from this file. This method reads a
     * `long` value as if by the `readLong` method
     * and then converts that `long` to a `double`
     * using the `longBitsToDouble` method in
     * class `Double`.
     *
     *
     * This method blocks until the eight bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return the next eight bytes of this file, interpreted as a
     * `double`.
     * @throws EOFException if this file reaches the end before reading
     * eight bytes.
     * @throws IOException  if an I/O error occurs.
     * @see java.io.RandomAccessFile.readLong
     * @see java.lang.Double.longBitsToDouble
     */
    @Throws(IOException::class)
    override fun readDouble(): Double {
        return java.lang.Double.longBitsToDouble(readLong())
    }

    /**
     * Read an array of doubles
     *
     * @param pa    read into this array
     * @param start starting at pa[start]
     * @param n     read this many elements
     * @throws IOException on read error
     */
    @Throws(IOException::class)
    fun readDouble(pa: DoubleArray, start: Int, n: Int) {
        for (i in 0 until n) {
            pa[start + i] = java.lang.Double.longBitsToDouble(readLong())
        }
    }


    /**
     * Reads the next line of text from this file.  This method successively
     * reads bytes from the file, starting at the current file pointer,
     * until it reaches a line terminator or the end
     * of the file.  Each byte is converted into a character by taking the
     * byte's value for the lower eight bits of the character and setting the
     * high eight bits of the character to zero.  This method does not,
     * therefore, support the full Unicode character set.
     *
     *
     *  A line of text is terminated by a carriage-return character
     * (`'&#92;r'`), a newline character (`'&#92;n'`), a
     * carriage-return character immediately followed by a newline character,
     * or the end of the file.  Line-terminating characters are discarded and
     * are not included as part of the string returned.
     *
     *
     *  This method blocks until a newline character is read, a carriage
     * return and the byte following it are read (to see if it is a newline),
     * the end of the file is reached, or an exception is thrown.
     *
     * @return the next line of text from this file, or null if end
     * of file is encountered before even one byte is read.
     * @exception IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun readLine(): String? {
        val input = StringBuilder()
        var c = -1
        var eol = false

        while (!eol) {
            c = read()
            when (c) {
                -1, NEW_LINE_CHAR -> eol = true
                CARET_RETURN_CHAR -> {
                    eol = true
                    val cur = filePointer
                    if (read() != NEW_LINE_CHAR) {
                        seek(cur)
                    }
                }
                else -> input.append(c.toChar())
            }
        }

        return when {
            c == -1 && input.isEmpty() -> null
            else -> input.toString()
        }
    }

    /**
     * Reads in a string from this file. The string has been encoded
     * using a modified UTF-8 format.
     *
     *
     * The first two bytes are read as if by
     * `readUnsignedShort`. This value gives the number of
     * following bytes that are in the encoded string, not
     * the length of the resulting string. The following bytes are then
     * interpreted as bytes encoding characters in the UTF-8 format
     * and are converted into characters.
     *
     *
     * This method blocks until all the bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @return a Unicode string.
     * @throws EOFException           if this file reaches the end before
     * reading all the bytes.
     * @throws IOException            if an I/O error occurs.
     * @throws UTFDataFormatException if the bytes do not represent
     * valid UTF-8 encoding of a Unicode string.
     * @see java.io.RandomAccessFile.readUnsignedShort
     */
    @Throws(IOException::class)
    override fun readUTF(): String {
        return DataInputStream.readUTF(this)
    }

    /**
     * Read a String of known length.
     *
     * @param nbytes number of bytes to read
     * @return String wrapping the bytes.
     * @throws IOException if an I/O error occurs.
     */
    @Throws(IOException::class)
    fun readString(nbytes: Int): String {
        val data = ByteArray(nbytes)
        readFully(data)
        return String(data, UTF8)
    }

    /**
     * Read a String of max length, zero terminate.
     *
     * @param nbytes number of bytes to read
     * @return String wrapping the bytes.
     * @throws IOException if an I/O error occurs.
     */
    @Throws(IOException::class)
    fun readStringMax(nbytes: Int): String {
        val b = ByteArray(nbytes)
        readFully(b)
        var count: Int
        count = 0
        while (count < nbytes) {
            if (b[count].toInt() == 0) break
            count++
        }
        return String(b, 0, count, UTF8)
    }

    /**
     * Skips exactly `n` bytes of input.
     * This method blocks until all the bytes are skipped, the end of
     * the stream is detected, or an exception is thrown.
     *
     * @param n the number of bytes to be skipped.
     * @return the number of bytes skipped, which is always `n`.
     * @throws EOFException if this file reaches the end before skipping
     * all the bytes.
     * @throws IOException  if an I/O error occurs.
     */
    @Throws(IOException::class)
    override fun skipBytes(n: Int): Int {
        seek(filePointer + n)
        return n
    }

    @Throws(IOException::class)
    fun skipBytes(n: Long): Long {
        seek(filePointer + n)
        return n
    }

    /**
     * Read directly from file, without going through the buffer.
     * All reading goes through here or readToByteChannel;
     *
     * @param pos    start here in the file
     * @param b      put data into this buffer
     * @param offset buffer offset
     * @param len    this number of bytes
     * @return actual number of bytes read
     * @throws IOException on io error
     */
    @Throws(IOException::class)
    protected fun read_(pos: Long, b: ByteArray, offset: Int, len: Int): Int {
        val raf = randomAccessFile ?: error("File is closed: $location")
        raf.seek(pos)
        val n = raf.read(b, offset, len)

        if (debugAccessField) {
            if (logRead)
                LOG.debug(" **read_ " + location + " = " + len + " bytes at " + pos + "; block = " + pos / buffer.size)
            seeksCounter.incrementAndGet()
            bytesReadCounter.addAndGet(len.toLong())
        }

        return n
    }


    /**
     * Create a string representation of this object.
     *
     * @return a string representation of the state of the object.
     */
    override fun toString(): String {
        return location
        /* return "fp=" + filePosition + ", bs=" + bufferStart + ", de="
            + dataEnd + ", ds=" + dataSize + ", bl=" + buffer.length
            + ", readonly=" + readonly + ", bm=" + bufferModified; */
    }

    @Suppress("ObjectPropertyName")
    companion object {
        private val LOG = LogManager.getLogger(RandomAccessFile::class.java)

        private const val NEW_LINE_CHAR = '\n'.toInt()
        private const val CARET_RETURN_CHAR = '\r'.toInt()

        val UTF8 = Charset.forName("UTF-8")

        protected const val defaultBufferSize = 8092  // The default buffer size, in bytes.

        ///////////////////////////////////////////////////////////////////////
        /**
         * Debugging, do not use.
         *
         * Debug leaks - keep track of open files
         */
        var debugLeaks = false
            set(value) {
                if (value) {
                    // Set counters to zero, set
                    totalOpenedFilesCounter.set(0)
                    maxOpenedFilesCounter.set(0)
                    _allFiles = HashSet(1000)
                }
                field = value
            }

        protected var _allFiles: MutableSet<String> = HashSet(1)
        /**
         * Debugging, do not use.
         *
         * @return list of all files used.
         */
        val allFiles: List<String>
            get() = _allFiles.toList().sorted()


        protected var _openedFiles: MutableList<String>
                // could keep map on file hashcode
                = Collections.synchronizedList(ArrayList())
        /**
         * Debugging, do not use.
         *
         * @return list of currently opened (not closed yet) files.
         */
        val openedFiles: List<String>
            get() = Collections.unmodifiableList(_openedFiles)

        val openedFilesCount: Int
            get() = _openedFiles.size

        private val totalOpenedFilesCounter = AtomicLong()
        /**
         * Number of files which were opened. Note: this number doesn't reflect number of
         * currently opened (active) resources
         */
        val totalOpenedFileCount: Long
            get() = totalOpenedFilesCounter.get()

        private val maxOpenedFilesCounter = AtomicInteger()
        /**
         * Maximum simultaneously opened files
         */
        val maxOpenFileCount: Int
            get() = maxOpenedFilesCounter.get()

        private var bytesReadCounter: AtomicLong = AtomicLong()
        /**
         * Debugging, do not use.
         *
         * @return number of bytes read
         */
        val debugNbytes: Long
            get() = bytesReadCounter.toLong()


        private var seeksCounter: AtomicInteger = AtomicInteger()
        /**
         * Debugging, do not use.
         *
         * @return number of seeks performed
         */
        val debugNseeks: Int
            get() = seeksCounter.toInt()

        /* Debugging, do not use.
        */
        var debugAccessField = false
            set(value) {
                if (value) {
                    seeksCounter = AtomicInteger()
                    bytesReadCounter = AtomicLong()
                }
                field = value
            }

        var logOpen = false
        var logRead = false
    }
}