package org.jbb.big;

import com.google.common.base.Preconditions;

import org.jetbrains.annotations.NotNull;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

public class StreamWithCompressedBlocks extends InflaterInputStream {

  private boolean closed;
  private long compressedBlockSize = 0;
  @NotNull private final DataInputStream dataStream;
  @NotNull private final DataInputStream compressedDataStream;

  public StreamWithCompressedBlocks(@NotNull final InputStream in) {
    super(in);

    dataStream = new DataInputStream(in);
    compressedDataStream = new DataInputStream(this);
  }

  @NotNull
  public DataInputStream getDataStream() {
    return isInCompressedBlock() ? compressedDataStream : dataStream;
  }

  // TODO: Could add "throws IOException" and use "ensureOpen()" here and in "endCompressedBlock()".
  public void startCompressedBlock(final long size) {
    Preconditions.checkArgument(size > 0);
    Preconditions.checkState(!isInCompressedBlock());

    compressedBlockSize = size;
    inf.reset();
  }

  public void endCompressedBlock() {
    ensureInCompressedBlock();
    Preconditions.checkState(inf.getBytesRead() == compressedBlockSize);

    compressedBlockSize = 0;
  }

  public boolean isInCompressedBlock() {
    return compressedBlockSize > 0;
  }

  @Override
  protected void fill() throws IOException {
    ensureOpen();
    ensureInCompressedBlock();

    final long unreadBlockSize = compressedBlockSize - inf.getBytesRead();

    len = in.read(buf, 0, (int) Math.min(unreadBlockSize, buf.length));
    if (len == -1) {
      throw new EOFException("Unexpected end of ZLIB input stream");
    }
    inf.setInput(buf, 0, len);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      super.close();
      closed = true;
    }
  }

  private void ensureOpen() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }

  private void ensureInCompressedBlock() throws IllegalStateException {
    Preconditions.checkState(isInCompressedBlock());
  }
}
