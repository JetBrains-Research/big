package org.jbb.big;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A byte order-aware seekable complement to {@link java.io.DataInputStream}.
 *
 * @author Sergei Lebedev
 * @since 11/03/15
 */
public class SeekableDataInput extends InputStream implements AutoCloseable, DataInput {
  /**
   * Defaults byte order to {@code ByteOrder.BIG_ENDIAN}.
   *
   * @see #of(Path, ByteOrder)
   */
  public static SeekableDataInput of(final Path path) throws FileNotFoundException {
    return of(path, ByteOrder.BIG_ENDIAN);
  }

  public static SeekableDataInput of(final Path path, final ByteOrder order)
      throws FileNotFoundException {
    return new SeekableDataInput(new RandomAccessFile(path.toFile(), "r"), order);
  }

  private final RandomAccessFile file;
  @NotNull private final StreamWithCompressedBlocks stream;
  private ByteOrder order;

  private SeekableDataInput(final RandomAccessFile file, final ByteOrder order) {
    this.file = Objects.requireNonNull(file);
    this.order = Objects.requireNonNull(order);

    // TODO: Buffered?
    stream = new StreamWithCompressedBlocks(Channels.newInputStream(file.getChannel()));
  }

  public ByteOrder order() {
    return order;
  }

  public void order(final ByteOrder order) {
    this.order = Objects.requireNonNull(order);
  }

  public void startCompressedBlock(final long size) {
    stream.startCompressedBlock(size);
  }

  public void endCompressedBlock() {
    stream.endCompressedBlock();
  }

  /** Guess byte order from a given big-endian {@code magic}. */
  public void guess(final int magic) throws IOException {
    final byte[] b = new byte[4];
    readFully(b);
    final int bigMagic = Ints.fromBytes(b[0], b[1], b[2], b[3]);
    if (bigMagic != magic) {
      final int littleMagic = Ints.fromBytes(b[3], b[2], b[1], b[0]);
      if (littleMagic != magic) {
        throw new IllegalStateException("bad signature");
      }

      order = ByteOrder.LITTLE_ENDIAN;
    } else {
      order = ByteOrder.BIG_ENDIAN;
    }
  }

  @Override
  public String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFully(final @NotNull byte[] buf) throws IOException {
    ByteStreams.readFully(this, buf);
  }

  @Override
  public void readFully(final @NotNull byte[] buf, final int offset, final int length)
      throws IOException {
    ByteStreams.readFully(this, buf, offset, length);
  }

  @Override
  public int skipBytes(final int n) throws IOException {
    return getUnderlyingDataStream().skipBytes(n);
  }

  public void seek(final long pos) throws IOException {
    file.seek(pos);
  }

  public long tell() throws IOException {
    return file.getFilePointer();
  }

  @Override
  public int available() throws IOException {
    return Ints.checkedCast(file.length() - tell());
  }

  @Override
  public void close() throws IOException {
    stream.close();
    file.close();
  }

  @Override
  public int read() throws IOException {
    return getUnderlyingDataStream().read();
  }

  @Override
  public boolean readBoolean() throws IOException {
    return getUnderlyingDataStream().readBoolean();
  }

  @Override
  public byte readByte() throws IOException {
    return getUnderlyingDataStream().readByte();
  }

  @Override
  public int readUnsignedByte() throws IOException {
    return getUnderlyingDataStream().readUnsignedByte();
  }

  @Override
  public char readChar() throws IOException {
    return getUnderlyingDataStream().readChar();
  }

  @Override
  public short readShort() throws IOException {
    final byte b1 = readAndCheckByte();
    final byte b2 = readAndCheckByte();
    return order == ByteOrder.BIG_ENDIAN
           ? Shorts.fromBytes(b1, b2)
           : Shorts.fromBytes(b2, b1);
  }

  @Override
  public int readUnsignedShort() throws IOException {
    final byte b1 = readAndCheckByte();
    final byte b2 = readAndCheckByte();
    return order == ByteOrder.BIG_ENDIAN
           ? Ints.fromBytes((byte) 0, (byte) 0, b1, b2)
           : Ints.fromBytes((byte) 0, (byte) 0, b2, b1);
  }

  @Override
  public int readInt() throws IOException {
    final byte b1 = readAndCheckByte();
    final byte b2 = readAndCheckByte();
    final byte b3 = readAndCheckByte();
    final byte b4 = readAndCheckByte();
    return order == ByteOrder.BIG_ENDIAN
           ? Ints.fromBytes(b1, b2, b3, b4)
           : Ints.fromBytes(b4, b3, b2, b1);
  }

  public long readUnsignedInt() throws IOException {
    final byte b1 = readAndCheckByte();
    final byte b2 = readAndCheckByte();
    final byte b3 = readAndCheckByte();
    final byte b4 = readAndCheckByte();
    return order == ByteOrder.BIG_ENDIAN
           ? Longs.fromBytes((byte) 0, (byte) 0, (byte) 0, (byte) 0, b1, b2, b3, b4)
           : Longs.fromBytes((byte) 0, (byte) 0, (byte) 0, (byte) 0, b4, b3, b2, b1);
  }

  @Override
  public long readLong() throws IOException {
    final byte b1 = readAndCheckByte();
    final byte b2 = readAndCheckByte();
    final byte b3 = readAndCheckByte();
    final byte b4 = readAndCheckByte();
    final byte b5 = readAndCheckByte();
    final byte b6 = readAndCheckByte();
    final byte b7 = readAndCheckByte();
    final byte b8 = readAndCheckByte();
    return order == ByteOrder.BIG_ENDIAN
           ? Longs.fromBytes(b1, b2, b3, b4, b5, b6, b7, b8)
           : Longs.fromBytes(b8, b7, b6, b5, b4, b3, b2, b1);
  }

  @Override
  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  @Override
  public @NotNull String readUTF() throws IOException {
    return getUnderlyingDataStream().readUTF();
  }

  private byte readAndCheckByte() throws IOException {
    final int b = getUnderlyingDataStream().read();
    if (b == -1) {
      throw new EOFException();
    }

    return (byte) b;
  }

  @NotNull
  private DataInputStream getUnderlyingDataStream() {
    return stream.getDataStream();
  }
}
