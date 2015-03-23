package org.jbb.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
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
   * Find value associated with key. Return BptNodeLeaf if it's found.
   */
  public static Optional<BPlusLeaf> find(final Path filePath,
                                         final BptHeader bptHeader,
                                         final String chromName)
      throws IOException {
    if (chromName.length() > bptHeader.keySize) {
      return Optional.empty();
    }
    // TODO: Кажется, нужно обрезать chromName по keySize. В таблице 10 речь про first keySize
    // characters of chromosome name
    // FIXME: А зачем нужна проверка на размер поинтера? Пример (valSize != bpt->valSize)
    // Интересно, как на дереве это отражается
    final Optional<BPlusLeaf> bptNodeLeaf;
    try (SeekableDataInput s = SeekableDataInput.of(filePath)) {
      s.order(bptHeader.byteOrder);
      bptNodeLeaf = rFindChromByName(s, bptHeader, bptHeader.rootOffset, chromName);
    }
    return bptNodeLeaf;
  }

  /**
   * Find value corresponding to key.
   */
  private static Optional<BPlusLeaf> rFindChromByName(final SeekableDataInput s,
                                                        final BptHeader bptHeader,
                                                        final long blockStart,
                                                        final String chromName) throws IOException {
    s.seek(blockStart);
    // Read node format
    final boolean isLeaf = s.readBoolean();
    s.readBoolean(); // reserved
    final short childCount = s.readShort();

    final byte[] keyBuf = new byte[bptHeader.keySize];
    final byte[] valBuf = new byte[bptHeader.valSize];
    if (isLeaf) {
      for (int i = 0; i < childCount; i++) {
        s.readFully(keyBuf);
        s.readFully(valBuf);
        if (chromName.equals(new String(keyBuf))) {
          final ByteBuffer b = ByteBuffer.wrap(valBuf).order(bptHeader.byteOrder);
          final int chromId = b.getInt();
          final int chromSize = b.getInt();
          return Optional.of(new BPlusLeaf(new String(keyBuf), chromId, chromSize));
        }
      }
      return Optional.empty();
    } else {
      s.readFully(keyBuf);
      long fileOffset = s.readLong();
      for (int i = 0; i < childCount; i++) {
        s.readFully(keyBuf);
        if (chromName.compareTo(new String(keyBuf)) < 0) {
          break;
        }
        fileOffset = s.readLong();
      }
      return rFindChromByName(s, bptHeader, fileOffset, chromName);
    }
  }
}
