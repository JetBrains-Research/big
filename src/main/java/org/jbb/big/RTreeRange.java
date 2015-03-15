package org.jbb.big;

import java.util.Objects;

/**
 * A range is a pair of genomic offsets.
 *
 * @author Sergei Lebedev
 * @date 16/03/15
 */
public class RTreeRange {
  /** Start offset (inclusive). */
  public final RTreeOffset left;
  /** End offset (exclusive). */
  public final RTreeOffset right;

  public RTreeRange(final RTreeOffset left, final RTreeOffset right) {
    this.left = left;
    this.right = right;
  }

  public boolean contains(final RTreeOffset other) {
    return left.compareTo(other) <= 0 && right.compareTo(other) > 0;
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
