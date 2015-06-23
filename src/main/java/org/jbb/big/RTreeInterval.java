package org.jbb.big;

import com.google.common.annotations.VisibleForTesting;

import java.util.Objects;

/**
 * A semi-closed interval.
 *
 * @author Sergei Lebedev
 * @since 16/03/15
 */
public class RTreeInterval {
  public static RTreeInterval of(final int chromIx, final int startOffset, final int endOffset) {
    return of(chromIx, startOffset, chromIx, endOffset);
  }

  @VisibleForTesting
  protected static RTreeInterval of(final int startChromIx, final int startOffset,
                                    final int endChromIx, final int endOffset) {
    return new RTreeInterval(new RTreeOffset(startChromIx, startOffset),
                             new RTreeOffset(endChromIx, endOffset));
  }

  /** Start offset (inclusive). */
  public final RTreeOffset left;
  /** End offset (exclusive). */
  public final RTreeOffset right;

  public RTreeInterval(final RTreeOffset left, final RTreeOffset right) {
    this.left = left;
    this.right = right;
  }

  public boolean overlaps(final RTreeInterval other) {
    return !(other.right.compareTo(left) <= 0
             || right.compareTo(other.left) <= 0);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final RTreeInterval other = (RTreeInterval) obj;
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
