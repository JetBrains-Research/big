package org.jbb.big;

import java.util.Objects;

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

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof RTreeIndexLeaf))
      return false;

    final RTreeIndexLeaf other = (RTreeIndexLeaf) obj;
    return other.dataOffset == dataOffset &&
           other.dataSize == dataSize;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
