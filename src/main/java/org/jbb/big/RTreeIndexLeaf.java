package org.jbb.big;

import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * Chromosome R-tree external node format
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
class RTreeIndexLeaf {
  public final RTreeInterval interval;
  public final long dataOffset;
  public final long dataSize;

  RTreeIndexLeaf(final RTreeInterval interval,
                 final long dataOffset, final long dataSize) {
    this.interval = interval;
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
    return other.interval.equals(interval) &&
           other.dataOffset == dataOffset &&
           other.dataSize == dataSize;
  }

  @Override
  public int hashCode() {
    return Objects.hash(interval, dataOffset, dataSize);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .addValue(interval)
        .add("dataOffset", dataOffset)
        .add("dataSize", dataSize)
        .toString();
  }
}
