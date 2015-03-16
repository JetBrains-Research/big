package org.jbb.big;

/**
 * Chromosome R-tree external node format
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
public class RTreeIndexLeaf {
  public final long dataOffset;
  public final long dataSize;

  public RTreeIndexLeaf(final long dataOffset, final long dataSize) {
    this.dataOffset = dataOffset;
    this.dataSize = dataSize;
  }
}
