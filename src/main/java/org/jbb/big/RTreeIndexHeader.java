package org.jbb.big;

import java.io.IOException;
import java.nio.ByteOrder;

/**
 * Chromosome R-tree index headers
 *
 * @author Sergey Zherevchuk
 */
public class RTreeIndexHeader {

  public static final int MAGIC = 0x2468ace0;

  public final ByteOrder byteOrder;
  public final int blockSize;
  public final long itemCount;
  public final int startChromIx;
  public final int startBase;
  public final int endChromIx;
  public final int endBase;
  public final long fileSize;
  public final int itemsPerSlot;
  public final long rootOffset; // position of root block of r tree


  public RTreeIndexHeader(final ByteOrder byteOrder, final int blockSize, final long itemCount,
                          final int startChromIx, final int startBase, final int endChromIx,
                          final int endBase, final long fileSize, final int itemsPerSlot,
                          final long rootOffset) {
    this.byteOrder = byteOrder;
    this.blockSize = blockSize;
    this.itemCount = itemCount;
    this.startChromIx = startChromIx;
    this.startBase = startBase;
    this.endChromIx = endChromIx;
    this.endBase = endBase;
    this.fileSize = fileSize;
    this.itemsPerSlot = itemsPerSlot;
    this.rootOffset = rootOffset;
  }

  // attach R tree index headers
  public static RTreeIndexHeader read(final SeekableStream s,
                                       final long unzoomedIndexOffset) throws IOException {
    s.seek(unzoomedIndexOffset);
    s.guess(MAGIC);

    final int blockSize = s.readInt();
    final long itemCount = s.readLong();
    final int startChromIx = s.readInt();
    final int startBase = s.readInt();
    final int endChromIx = s.readInt();
    final int endBase = s.readInt();
    final long fileSize = s.readLong();
    final int itemsPerSlot = s.readInt();
    s.readInt(); // skip reserved bytes
    final long rootOffset = s.tell();

    return new RTreeIndexHeader(s.order(), blockSize, itemCount, startChromIx, startBase,
                                endChromIx, endBase, fileSize, itemsPerSlot, rootOffset);
  }
}


