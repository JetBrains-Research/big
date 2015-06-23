package org.jbb.big;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Objects;

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
class RTreeIndex {

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

    public static long write(final SeekableDataOutput writer, final Path chromSizesPath,
                             final Path bedFilePath, final int blockSize,
                             final int itemsPerSlot, final short fieldCount) throws IOException {
      final Hashtable<String, Integer> chromSizesHash = RTreeIndexDetails.bbiChromSizesFromFile(chromSizesPath);
      final wrapObject minDiff = new wrapObject();
      final wrapObject aveSize = new wrapObject(), bedCount = new wrapObject();
      final ArrayList<bbiChromUsage> usageList = RTreeIndexDetails.bbiChromUsageFromBedFile(bedFilePath,
                                                                                      chromSizesHash,
                                                                                      minDiff,
                                                                                      aveSize,
                                                                                      bedCount);

      final int resScales[] = new int[RTreeIndexDetails.bbiMaxZoomLevels];
      final int resSizes[] = new int[RTreeIndexDetails.bbiMaxZoomLevels];
      final int resTryCount = RTreeIndexDetails.bbiCalcResScalesAndSizes(aveSize.toInt(), resScales,
                                                                   resSizes);


      final int blockCount = RTreeIndexDetails.bbiCountSectionsNeeded(usageList, itemsPerSlot);
      final bbiBoundsArray boundsArray[] = new bbiBoundsArray[blockCount];
      for (int i = 0; i < blockCount; i++ ) boundsArray[i] = new bbiBoundsArray();

      final boolean doCompress = false;

      final wrapObject maxBlockSize = new wrapObject();


      RTreeIndexDetails.writeBlocks(usageList, bedFilePath, itemsPerSlot, boundsArray, blockCount,
                                    doCompress, writer,
                                    resTryCount, resScales, resSizes,
                                    bedCount.toInt(), fieldCount, maxBlockSize);

    /* Write out primary data index. */
      final long indexOffset = writer.tell();
      final int itemSize = 24;
      RTreeIndexDetails.cirTreeFileBulkIndexToOpenFile(boundsArray, itemSize, blockCount,
                                                       blockSize, 1,
                                                       indexOffset, writer);

      return indexOffset;
    }
  }

  public final Header header;

  public RTreeIndex(final Header header) {
    this.header = Objects.requireNonNull(header);
  }

  /**
   * Recursively traverses an R+ tree calling {@code consumer} on each
   * block (aka leaf) overlapping a given {@code query}. Note that some
   * of the intervals contained in a block might *not* overlap the
   * {@code query}.
   */
  protected void findOverlappingBlocks(final SeekableDataInput s, final RTreeInterval query,
                                       final UnsafeConsumer<RTreeIndexLeaf> consumer)
      throws IOException {
    final ByteOrder originalOrder = s.order();
    s.order(header.byteOrder);
    try {
      findOverlappingBlocksRecursively(s, query, header.rootOffset, consumer);
    } finally {
      s.order(originalOrder);
    }
  }

  private void findOverlappingBlocksRecursively(final SeekableDataInput s,
                                                final RTreeInterval query,
                                                final long offset,
                                                final UnsafeConsumer<RTreeIndexLeaf> consumer)
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
          final long backup = s.tell();
          consumer.consume(new RTreeIndexLeaf(dataOffset, dataSize));
          s.seek(backup);
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
        findOverlappingBlocksRecursively(s, query, node.dataOffset, consumer);
      }
    }
  }
}
