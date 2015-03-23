package org.jbb.big;

import java.util.Objects;

/**
 * A semi-closed interval.
 *
 * @author Sergei Lebedev
 * @since 16/03/15
 */
public class RTreeRange {
  public static RTreeRange of(final int chromIx, final int startOffset, final int endOffset) {
    return new RTreeRange(new RTreeOffset(chromIx, startOffset),
                          new RTreeOffset(chromIx, endOffset));
  }

  /** Start offset (inclusive). */
  public final RTreeOffset left;
  /** End offset (exclusive). */
  public final RTreeOffset right;

  public RTreeRange(final RTreeOffset left, final RTreeOffset right) {
    this.left = left;
    this.right = right;
  }

  public boolean overlaps(final RTreeRange other) {
    return left.compareTo(other.left) >= 0 && right.compareTo(other.right) <= 0;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final RTreeRange other = (RTreeRange) obj;
    return Objects.equals(left, other.left) && Objects.equals(right, other.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(left, right);
  }

  @Override
  public String toString() {
    return "[" + left + ';' + right + ')';
  }
}
