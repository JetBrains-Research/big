package org.jbb.big;

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
}
