package org.jbb.big;

/**
 * Internal node of the chromosome R-tree.
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
class RTreeIndexNode {
  public final RTreeInterval interval;
  public final long dataOffset;

  RTreeIndexNode(final RTreeInterval interval, final long dataOffset) {
    this.interval = interval;
    this.dataOffset = dataOffset;
  }
}
