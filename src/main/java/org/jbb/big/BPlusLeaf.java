package org.jbb.big;

/**
 * A leaf node in a B+ tree.
 *
 * @author Sergey Zherevchuk
 * @since 10/03/15
 */
public class BPlusLeaf {
  /** Chromosome name, e.g. "chr19" or "chrY". */
  public final String key;
  /** Unique chromosome identifier. */
  public final int id;
  /** Chromosome length in base pairs. */
  public final int size;

  public BPlusLeaf(final String key, final int id, final int size) {
    this.key = key;
    this.id = id;
    this.size = size;
  }

  @Override
  public String toString() {
    return String.format("[%s] => [%d; %d]", key, id, size);
  }

  @Override
  public boolean equals(Object obj) {
    BPlusLeaf other = (BPlusLeaf) obj;
    return this.key.equals(other.key) &&
           this.id == other.id &&
           this.size == other.size;
  }
}
