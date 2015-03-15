package org.jbb.big;

/**
 * Chromosome R-tree external node format
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
public class RTreeIndexNodeLeaf {

  public final long dataOffset; // FIXME: from the begining of file?
  public final long dataSize;

  public RTreeIndexNodeLeaf(final long dataOffset, final long dataSize) {
    this.dataOffset = dataOffset;
    this.dataSize = dataSize;
  }
}
