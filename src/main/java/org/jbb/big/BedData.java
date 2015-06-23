package org.jbb.big;

import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * A minimal representation of a BED file entry.
 *
 * @author Sergey Zherevchik
 * @since 15/03/15
 */
public class BedData {
  /** Chromosome id as defined by B+ tree. */
  public final int id;
  /** 0-based start offset (inclusive). */
  public final int start;
  /** 0-based end offset (exclusive). */
  public final int end;
  /** Comma-separated string of additional BED values. */
  public final String rest;

  public BedData(final int id, final int start, final int end, final String rest) {
    this.id = id;
    this.start = start;
    this.end = end;
    this.rest = rest;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final BedData other = (BedData) obj;
    return id == other.id && start == other.start && end == other.end &&
           rest.equals(other.rest);

  }

  @Override
  public int hashCode() {
    return Objects.hash(id, start, end, rest);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id)
        .add("start", start)
        .add("end", end)
        .toString();
  }
}
