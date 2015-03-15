package org.jbb.big;

import com.google.common.collect.ComparisonChain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Chromosome R-tree index class
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
public class RTreeIndex {

  /**
   * Return list of file blocks that between them contain all items that overlap
   * start/end on chromIx.  Also there will be likely some non-overlapping items
   * in these blocks too.
   */
  public static void rFindOverlappingBlocks(
      final LinkedList<RTreeIndexNodeLeaf> overlappingBlockList,
      final SeekableDataInput s,
      final int level,
      final long indexFileOffset,
      final int chromIx,
      final int chromStart,
      final int chromEnd) throws IOException {
    s.seek(indexFileOffset);
    final boolean isLeaf = s.readBoolean();
    s.readBoolean(); // reserved
    final short childCount = s.readShort();
    if (isLeaf) {
      // Loop through node adding overlapping leaves to block list.
      for (int i = 0; i < childCount; i++) {
        final int startChromIx = s.readInt();
        final int startBase = s.readInt();
        final int endChromIx = s.readInt();
        final int endBase = s.readInt();
        final long offset = s.readLong();
        final long size = s.readLong();
        if (overlaps(chromIx, chromStart, chromEnd, startChromIx, startBase, endChromIx, endBase)) {
          overlappingBlockList.addFirst(new RTreeIndexNodeLeaf(offset, size));
        }
      }
    } else {
      final ArrayList<RTreeIndexNodeInternal> childsList = new ArrayList<>(childCount);
      for (int i = 0; i < childCount; i++) {
        final int startChromIx = s.readInt();
        final int startBase = s.readInt();
        final int endChromIx = s.readInt();
        final int endBase = s.readInt();
        final long offset = s.readLong();
        childsList.add(
            new RTreeIndexNodeInternal(startChromIx, startBase, endChromIx, endBase, offset)
        );
      }
      // Recurse into child nodes that we overlap.
      for (final RTreeIndexNodeInternal node: childsList) {
        if (overlaps(chromIx, chromStart, chromEnd, node.startChromIx, node.startBase,
                     node.endChromIx, node.endBase)) {
          rFindOverlappingBlocks(overlappingBlockList, s, level + 1, node.dataOffset, chromIx,
                                 chromStart, chromEnd);

        }
      }
    }
  }

  /**
   * Check leave overlapping with query interval and chromId
   * @param qChrom query chromosome id
   * @param qStart query chromosome start restriction
   * @param qEnd query chromosome end restriction
   * @param rStartChrom ID of first chromosome in item
   * @param rStartBase Position of first base in item
   * @param rEndChrom ID of last chromosome in item
   * @param rEndBase Position of last base in item
   * @return boolean
   */
  public static boolean overlaps(final int qChrom, final int qStart, final int qEnd,
                                 final int rStartChrom, final int rStartBase, final int rEndChrom,
                                 final int rEndBase) {
    return cmpTwoInt(qChrom, qStart, rEndChrom, rEndBase) > 0 &&
           cmpTwoInt(qChrom, qEnd, rStartChrom, rStartBase) < 0;
  }

  /**
   * Return - if b is less than a , 0 if equal, else +
   */
  public static int cmpTwoInt(final int aHi, final int aLo, final int bHi, final int bLo) {
    return ComparisonChain.start()
        .compare(bHi, aHi)
        .compare(bLo, aLo)
        .result();
  }
}
