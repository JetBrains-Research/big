package org.jbb.big;

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
      final LinkedList<RTreeIndexLeaf> overlappingBlockList,
      final SeekableDataInput s,
      final int level,
      final long indexFileOffset,
      final RTreeRange query) throws IOException {
    s.seek(indexFileOffset);
    final boolean isLeaf = s.readBoolean();
    s.readBoolean(); // reserved
    final short childCount = s.readShort();
    if (isLeaf) {
      // Loop through node adding overlapping leaves to block list.
      for (int i = 0; i < childCount; i++) {
        final int startChromIx = s.readInt();
        final int startOffset = s.readInt();
        final int endChromIx = s.readInt();
        final int endOffset = s.readInt();
        final long dataOffset = s.readLong();
        final long dataSize = s.readLong();
        final RTreeIndexNode node
            = RTreeIndexNode.of(startChromIx, startOffset, endChromIx, endOffset, dataOffset);
        if (node.overlaps(query)) {
          overlappingBlockList.addFirst(new RTreeIndexLeaf(dataOffset, dataSize));
        }
      }
    } else {
      final ArrayList<RTreeIndexNode> childsList = new ArrayList<>(childCount);
      for (int i = 0; i < childCount; i++) {
        final int startChromIx = s.readInt();
        final int startBase = s.readInt();
        final int endChromIx = s.readInt();
        final int endBase = s.readInt();
        final long offset = s.readLong();
        childsList.add(RTreeIndexNode.of(startChromIx, startBase, endChromIx, endBase, offset));
      }

      // Recurse into child nodes that we overlap.
      for (final RTreeIndexNode node : childsList) {
        if (node.overlaps(query)) {
          rFindOverlappingBlocks(overlappingBlockList, s, level + 1, node.dataOffset, query);
        }
      }
    }
  }
}
