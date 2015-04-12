package org.jbb.big;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * A 1-D R+ tree for storing genomic intervals.
 *
 * TODO: explain that we don't index all intervals when building a tree.
 * TODO: explain that the 1-D R+ tree is simply a range tree build with
 *       using interval union.
 *
 * See tables 14-17 in the Supplementary Data for byte-to-byte details
 * on the R+ tree header and node formats.
 *
 * @author Sergey Zherevchuk
 * @author Sergei Lebedev
 * @since 13/03/15
 */
public class RTreeIndex {

  public static RTreeIndex read(final SeekableDataInput s, final long offset)
      throws IOException {
    final Header header = Header.read(s, offset);
    return new RTreeIndex(header);
  }

  @VisibleForTesting
  protected static class Header {

    private static final int MAGIC = 0x2468ace0;

    protected final ByteOrder byteOrder;
    protected final int blockSize;
    protected final long itemCount;
    protected final int startChromIx;
    protected final int startBase;
    protected final int endChromIx;
    protected final int endBase;
    protected final long fileSize;
    protected final int itemsPerSlot;
    protected final long rootOffset; // position of root block of r tree

    Header(final ByteOrder byteOrder, final int blockSize, final long itemCount,
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

    public static Header read(final SeekableDataInput s, final long offset)
        throws IOException {
      s.seek(offset);
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

      return new Header(s.order(), blockSize, itemCount, startChromIx, startBase,
                        endChromIx, endBase, fileSize, itemsPerSlot, rootOffset);
    }
  }

  protected final Header header;

  public RTreeIndex(final Header header) {
    this.header = Objects.requireNonNull(header);
  }

  /**
   * Executes a possibly unsafe computation using a correct byte order
   * for {@code s}. Original byte order is restored after the {@code callable}
   * has completed (either normally or by throwing an exception).
   */
  <T> T within(final SeekableDataInput s, final Callable<T> callable)
      throws IOException {
    final ByteOrder originalOrder = s.order();
    s.order(header.byteOrder);

    try {
      return callable.call();
    } catch (final Exception e) {
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new RuntimeException(e);
      }
    } finally {
      s.order(originalOrder);
    }
  }

  /**
   * Returns a list of R+ tree blocks (aka leaves) overlapping a given
   * {@code query}. Note that since some of the intervals contained in
   * a block might *not* overlap the {@code query}.
   */
  protected List<RTreeIndexLeaf> findOverlappingBlocks(
      final SeekableDataInput s, final RTreeInterval query)
      throws IOException {
    return within(s, () -> {
      final List<RTreeIndexLeaf> overlappingBlocks = Lists.newArrayList();
      findOverlappingBlocksRecursively(s, query, header.rootOffset, overlappingBlocks);
      return overlappingBlocks;
    });
  }

  private void findOverlappingBlocksRecursively(
      final SeekableDataInput s, final RTreeInterval query, final long offset,
      final List<RTreeIndexLeaf> overlappingBlocks)
      throws IOException {
    // Invariant: a stream is in Header.byteOrder.
    s.seek(offset);

    final boolean isLeaf = s.readBoolean();
    s.readBoolean(); // reserved
    final short childCount = s.readShort();

    if (isLeaf) {
      for (int i = 0; i < childCount; i++) {
        final int startChromIx = s.readInt();
        final int startOffset = s.readInt();
        final int endChromIx = s.readInt();
        final int endOffset = s.readInt();
        final long dataOffset = s.readLong();
        final long dataSize = s.readLong();
        final RTreeIndexNode node
            = RTreeIndexNode.of(startChromIx, startOffset, endChromIx, endOffset,
                                dataOffset);
        if (node.overlaps(query)) {
          overlappingBlocks.add(new RTreeIndexLeaf(dataOffset, dataSize));
        }
      }
    } else {
      final List<RTreeIndexNode> children = new ArrayList<>(childCount);
      for (int i = 0; i < childCount; i++) {
        final int startChromIx = s.readInt();
        final int startBase = s.readInt();
        final int endChromIx = s.readInt();
        final int endBase = s.readInt();
        final long dataOffset = s.readLong();
        final RTreeIndexNode node = RTreeIndexNode.of(
            startChromIx, startBase, endChromIx, endBase, dataOffset);

        // XXX only add overlapping children, because there's no point
        // in storing all of them.
        if (node.overlaps(query)) {
          children.add(node);
        }
      }

      for (final RTreeIndexNode node : children) {
        findOverlappingBlocksRecursively(s, query, node.dataOffset, overlappingBlocks);
      }
    }
  }
}
