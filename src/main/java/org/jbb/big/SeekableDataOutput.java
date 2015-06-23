package org.jbb.big;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import org.jetbrains.annotations.NotNull;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A byte order-aware seekable complement to {@link java.io.DataOutputStream}.
 *
 * @author Belyaev Igor
 * @since 06/04/15
 */
public class SeekableDataOutput extends OutputStream implements AutoCloseable, DataOutput {

  /**
   * Defaults byte order to {@code ByteOrder.BIG_ENDIAN}.
   *
   * @see #of(Path, ByteOrder)
   */
  public static SeekableDataOutput of(final Path path) throws IOException {
    return of(path, ByteOrder.BIG_ENDIAN);
  }

  public static SeekableDataOutput of(final Path path, final ByteOrder order)
      throws IOException {
    return new SeekableDataOutput(new RandomAccessFile(path.toFile(), "rw"), order);
  }

  private final RandomAccessFile file;
  private ByteOrder order;

  private SeekableDataOutput(final RandomAccessFile file, final ByteOrder order) {
    this.file = Objects.requireNonNull(file);
    this.order = Objects.requireNonNull(order);
  }

  public ByteOrder order() {
    return order;
  }

  public void order(final ByteOrder order) {
    this.order = Objects.requireNonNull(order);
  }

  public void skipBytes(final int n) throws IOException {
    file.skipBytes(n);
  }

  public void seek(final long pos) throws IOException {
    file.seek(pos);
  }

  public long tell() throws IOException {
    return file.getFilePointer();
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  @Override
  public void writeBoolean(final boolean v) throws IOException {
    file.writeBoolean(v);
  }

  @Override
  public void writeByte(final int v) throws IOException {
    file.writeByte(v);
  }

  public void writeByte(final int v, final int count) throws IOException {
    for (int i = 0; i < count; i++) {
      writeByte(v);
    }
  }

  @Override
  public void writeShort(final int v) throws IOException {
    final byte b[] = Shorts.toByteArray((short) v);
    file.writeShort(order == ByteOrder.BIG_ENDIAN
                    ? Shorts.fromBytes(b[0], b[1])
                    : Shorts.fromBytes(b[1], b[0]));
  }

  public void writeUnsignedShort(final int v) throws IOException {
    final byte b[] = Ints.toByteArray(v);
    file.writeShort(order == ByteOrder.BIG_ENDIAN
                    ? Shorts.fromBytes(b[2], b[3])
                    : Shorts.fromBytes(b[3], b[2]));
  }

  @Override
  public void writeChar(final int v) throws IOException {
    file.writeChar(v);
  }

  @Override
  public void writeInt(final int v) throws IOException {
    final byte[] b = Ints.toByteArray(v);
    file.writeInt(order == ByteOrder.BIG_ENDIAN
                  ? Ints.fromBytes(b[0], b[1], b[2], b[3])
                  : Ints.fromBytes(b[3], b[2], b[1], b[0]));
  }

  public void writeUnsignedInt(final long v) throws IOException {
    final byte[] b = Longs.toByteArray(v);
    file.writeInt(order == ByteOrder.BIG_ENDIAN
                  ? Ints.fromBytes(b[4], b[5], b[6], b[7])
                  : Ints.fromBytes(b[7], b[6], b[5], b[4]));
  }

  @Override
  public void writeLong(final long v) throws IOException {
    final byte[] b = Longs.toByteArray(v);
    file.writeLong(order == ByteOrder.BIG_ENDIAN
                   ? Longs.fromBytes(b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7])
                   : Longs.fromBytes(b[7], b[6], b[5], b[4], b[3], b[2], b[1], b[0]));
  }

  @Override
  public void writeFloat(final float v) throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public void writeDouble(final double v) throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  @Override
  public void writeBytes(final @NotNull String s) throws IOException {
    file.writeBytes(s);
  }

  public void writeBytes(final @NotNull String s, final int length) throws IOException {
    file.writeBytes(s);
    writeByte(0, length - s.length());
  }

  @Override
  public void writeChars(final @NotNull String s) throws IOException {
    file.writeChars(s);
  }

  @Override
  public void writeUTF(final @NotNull String s) throws IOException {
    file.writeUTF(s);
  }

  @Override
  public void write(final int b) throws IOException {
    file.write(b);
  }
}
