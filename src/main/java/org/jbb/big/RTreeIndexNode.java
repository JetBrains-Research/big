package org.jbb.big;

import com.google.common.primitives.Ints;

/**
 * Internal node of the chromosome R-tree.
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
public class RTreeIndexNode extends RTreeRange {
  // XXX replace this with #readFrom(SeekableDataInput)?
  public static RTreeIndexNode of(final int startChromIx, final int startOffset,
                                  final int endChromIx, final int endOffset,
                                  final long dataOffset) {
    return new RTreeIndexNode(new RTreeOffset(startChromIx, startOffset),
                              new RTreeOffset(endChromIx, endOffset),
                              Ints.checkedCast(dataOffset));
  }

  public final long dataOffset;

  private RTreeIndexNode(final RTreeOffset left, final RTreeOffset right, final int dataOffset) {
    super(left, right);

    this.dataOffset = dataOffset;
  }
}
