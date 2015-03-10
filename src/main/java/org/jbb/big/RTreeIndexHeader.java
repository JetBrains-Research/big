package org.jbb.big;

import com.google.common.primitives.Ints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * Chromosome R-tree index headers
 *
 * @author Sergey Zherevchuk
 */
public class RTreeIndexHeader {

  public static final int MAGIC = 0x2468ace0;
  public static final int HEADER_SIZE = 48;

  public ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
  final int blockSize;
  final long itemCount;
  final int startChromIx;
  final int startBase;
  final int endChromIx;
  final int endBase;
  final long fileSize;
  final int itemsPerSlot;
  final long rootOffset; // position of root block of r tree


  public RTreeIndexHeader(ByteOrder byteOrder, int blockSize, long itemCount, int startChromIx,
                          int startBase, int endChromIx, int endBase, long fileSize,
                          int itemsPerSlot, long rootOffset) {
    this.blockSize = blockSize;
    this.itemCount = itemCount;
    this.startChromIx = startChromIx;
    this.startBase = startBase;
    this.endChromIx = endChromIx;
    this.endBase = endBase;
    this.fileSize = fileSize;
    this.itemsPerSlot = itemsPerSlot;
    this.rootOffset = rootOffset;
    this.byteOrder = byteOrder;
  }

  // attach R tree index headers
  public static RTreeIndexHeader parse(FileChannel fc,
                                       final long unzoomedIndexOffset) throws IOException {
    fc.position(unzoomedIndexOffset);
    ByteBuffer rb = ByteBuffer.allocate(HEADER_SIZE);
    fc.read(rb);
    rb.flip();
    // Set byte order
    final byte[] b = new byte[4];
    ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
    rb.get(b);
    final int bigMagic = Ints.fromBytes(b[0], b[1], b[2], b[3]);
    if (bigMagic != MAGIC) {
      final int littleMagic = Ints.fromBytes(b[3], b[2], b[1], b[0]);
      if (littleMagic != MAGIC) {
        throw new IllegalStateException("bad signature R tree");
      }
      byteOrder = ByteOrder.LITTLE_ENDIAN;
    }
    rb.order(byteOrder);

    final int blockSize = rb.getInt();
    final long itemCount = rb.getLong();
    final int startChromIx = rb.getInt();
    final int startBase = rb.getInt();
    final int endChromIx = rb.getInt();
    final int endBase = rb.getInt();
    final long fileSize = rb.getLong();
    final int itemsPerSlot = rb.getInt();
    rb.getInt(); // skip reserved bytes
    final long rootOffset = fc.position();

    return new RTreeIndexHeader(byteOrder, blockSize, itemCount, startChromIx, startBase,
                                endChromIx, endBase, fileSize, itemsPerSlot, rootOffset);
  }
}
