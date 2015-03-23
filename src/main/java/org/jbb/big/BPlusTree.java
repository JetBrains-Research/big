package org.jbb.big;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.function.Consumer;


/**
 * A B+ tree.
 *
 * Big formats use a B+ tree to store a mapping from chromosome
 * names to (id, size) pairs, where id is a unique positive integer
 * and size is chromosome length in base pairs.
 *
 * Contrary to the original definition the leaves in this B+ tree
 * aren't linked.
 *
 * @author Sergey Zherevchuk
 * @author Sergei Lebedev
 * @since 13/03/15
 */
public class BPlusTree {

  /**
   * Recursively goes across tree, calling callback on the leaves.
   */
  public static void traverse(final SeekableDataInput s, final BptHeader bptHeader,
                              final Consumer<BPlusLeaf> consumer)
      throws IOException {
    final ByteOrder originalOrder = s.order();
    s.order(bptHeader.byteOrder);
    traverseRecursively(s, bptHeader, bptHeader.rootOffset, consumer);
    s.order(originalOrder);
  }

  private static void traverseRecursively(final SeekableDataInput s,
                                          final BptHeader bptHeader,
                                          final long blockStart,
                                          final Consumer<BPlusLeaf> consumer)
      throws IOException {
    // Invariant: a stream is in bptHeader.byteOrder.
    s.seek(blockStart);

    final boolean isLeaf = s.readBoolean();
    s.readBoolean(); // reserved
    final short childCount = s.readShort();

    final byte[] keyBuf = new byte[bptHeader.keySize];
    if (isLeaf) {
      for (int i = 0; i < childCount; i++) {
        s.readFully(keyBuf);
        final int chromId = s.readInt();
        final int chromSize = s.readInt();
        consumer.accept(new BPlusLeaf(new String(keyBuf), chromId, chromSize));
      }
    } else {
      final long fileOffsets[] = new long[childCount];
      for (int i = 0; i < childCount; i++) {
        s.readFully(keyBuf);  // XXX why can we overwrite it?
        fileOffsets[i] = s.readLong();
      }

      for (int i = 0; i < childCount; i++) {
        traverseRecursively(s, bptHeader, fileOffsets[i], consumer);
      }
    }
  }

  /**
   * Recursively traverses a B+ tree looking for a leaf corresponding
   * to {@code query}.
   */
  public static Optional<BPlusLeaf> find(final SeekableDataInput s,
                                         final BptHeader bptHeader,
                                         final String query)
      throws IOException {
    if (query.length() > bptHeader.keySize) {
      return Optional.empty();
    }

    // FIXME: А зачем нужна проверка на размер поинтера? Пример (valSize != bpt->valSize)
    // Интересно, как на дереве это отражается
    final ByteOrder originalOrder = s.order();
    s.order(bptHeader.byteOrder);

    // Trim query to 'keySize' because the spec. guarantees us
    // that all B+ tree nodes have a fixed-size key.
    final String trimmedQuery
        = query.substring(0, Math.min(query.length(), bptHeader.keySize));
    final Optional<BPlusLeaf> res = findRecursively(s, bptHeader,
                                                    bptHeader.rootOffset,
                                                    trimmedQuery);
    s.order(originalOrder);
    return res;
  }

  private static Optional<BPlusLeaf> findRecursively(final SeekableDataInput s,
                                                     final BptHeader bptHeader,
                                                     final long blockStart,
                                                     final String query)
      throws IOException {
    // Invariant: a stream is in bptHeader.byteOrder.
    s.seek(blockStart);

    final boolean isLeaf = s.readBoolean();
    s.readBoolean(); // reserved
    final short childCount = s.readShort();

    final byte[] keyBuf = new byte[bptHeader.keySize];
    if (isLeaf) {
      for (int i = 0; i < childCount; i++) {
        s.readFully(keyBuf);
        final int chromId = s.readInt();
        final int chromSize = s.readInt();

        final String key = new String(keyBuf);
        if (query.equals(key)) {
          return Optional.of(new BPlusLeaf(key, chromId, chromSize));
        }
      }

      return Optional.empty();
    } else {
      s.readFully(keyBuf);
      long fileOffset = s.readLong();
      for (int i = 0; i < childCount; i++) {
        s.readFully(keyBuf);
        if (query.compareTo(new String(keyBuf)) < 0) {
          break;
        }

        fileOffset = s.readLong();
      }

      return findRecursively(s, bptHeader, fileOffset, query);
    }
  }
}
