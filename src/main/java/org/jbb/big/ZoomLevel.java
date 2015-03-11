package org.jbb.big;

/**
 * @author Sergei Lebedev
 * @since 11/03/15
 */
public class ZoomLevel {
  public final int reductionLevel;
  public final int reserved;  // currently 0.
  public final long dataOffset;
  public final long indexOffset;


  public ZoomLevel(final int reductionLevel, final int reserved,
                   final long dataOffset, final long indexOffset) {
    this.reductionLevel = reductionLevel;
    this.reserved = reserved;
    this.dataOffset = dataOffset;
    this.indexOffset = indexOffset;
  }
}
