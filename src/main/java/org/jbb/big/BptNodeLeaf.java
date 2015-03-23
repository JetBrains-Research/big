package org.jbb.big;

/**
 * A single node in a B+ tree.
 *
 * Big formats use a B+ tree to store a mapping from chromosome
 * names to (id, size) pairs, where id is a unique positive integer
 * and size is chromosome length in base pairs.
 *
 * @author Sergey Zherevchuk
 * @since 10/03/15
 */
public class BptNodeLeaf {
  /** Chromosome name, e.g. "chr19" or "chrY". */
  public final String key;
  /** Unique chromosome identifier. */
  public final int id;
  /** Chromosome length in base pairs. */
  public final int size;

  public BptNodeLeaf(final String key, final int id, final int size) {
    this.key = key;
    this.id = id;
    this.size = size;
  }
}
