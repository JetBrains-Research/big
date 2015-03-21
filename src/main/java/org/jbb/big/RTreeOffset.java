package org.jbb.big;

import com.google.common.collect.ComparisonChain;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * A (chromosome, offset) pair.
 *
 * @author Sergei Lebedev
 * @date 16/03/15
 */
public class RTreeOffset implements Comparable<RTreeOffset> {
  /** Chromosome ID as defined by the B+ index. */
  public final int chromIx;
  /** 0-based genomic offset. */
  public final int offset;

  public RTreeOffset(final int chromIx, final int offset) {
    this.chromIx = chromIx;
    this.offset = offset;
  }

  @Override
  public int compareTo(final @NotNull RTreeOffset other) {
    return ComparisonChain.start()
        .compare(chromIx, other.chromIx)
        .compare(offset, other.offset)
        .result();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final RTreeOffset other = (RTreeOffset) obj;
    return chromIx == other.chromIx && offset == other.offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(chromIx, offset);
  }

  @Override
  public String toString() {
    return String.valueOf(chromIx) + ':' + offset;
  }
}
