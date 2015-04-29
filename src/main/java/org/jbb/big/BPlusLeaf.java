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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final BPlusLeaf bPlusLeaf = (BPlusLeaf) o;

    return id == bPlusLeaf.id &&
           size == bPlusLeaf.size &&
           key.equals(bPlusLeaf.key);
  }

  @Override
  public int hashCode() {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + id;
    result = 31 * result + size;
    return result;
  }

  @Override
  public String toString() {
    return String.format("[%s] => [%d; %d]", key, id, size);
  }

}