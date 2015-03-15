package org.jbb.big;

/**
 * Chromosome R-tree internal node format
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
public class RTreeIndexNodeInternal {

  public final int startChromIx;
  public final int startBase;
  public final int endChromIx;
  public final int endBase;
  public final long dataOffset;

  public RTreeIndexNodeInternal(int startChromIx, int startBase, int endChromIx, int endBase,
                                long dataOffset) {
    this.startChromIx = startChromIx;
    this.startBase = startBase;
    this.endChromIx = endChromIx;
    this.endBase = endBase;
    this.dataOffset = dataOffset;
  }
}
