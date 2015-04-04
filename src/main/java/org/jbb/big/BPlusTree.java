package org.jbb.big;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;


/**
 * A B+ tree mapping chromosome names to (id, size) pairs.
 *
 * Here {@code id} is a unique positive integer and size is
 * chromosome length in base pairs. Contrary to the original
 * definition the leaves in this B+ tree aren't linked.
 *
 * See tables 8-11 in Supplementary Data for byte-to-byte details
 * on the B+ header and node formats.
 *
 * @author Sergey Zherevchuk
 * @author Sergei Lebedev
 * @since 13/03/15
 */
public class BPlusTree {

  public static BPlusTree read(final SeekableDataInput s, final long offset)
      throws IOException {
    final Header header = Header.read(s, offset);
    return new BPlusTree(header);
  }

  @VisibleForTesting
  protected static class Header {

    private static final int MAGIC = 0x78ca8c91;

    protected final ByteOrder byteOrder;
    protected final int blockSize;
    protected final int keySize;
    protected final int valSize;
    protected final long itemCount;
    protected final long rootOffset;

    Header(final ByteOrder byteOrder, final int blockSize, final int keySize,
                  final int valSize, final long itemCount, final long rootOffset) {
      this.byteOrder = byteOrder;
      this.blockSize = blockSize;
      this.keySize = keySize;
      this.valSize = valSize;
      this.itemCount = itemCount;
      this.rootOffset = rootOffset;
    }

    static Header read(final SeekableDataInput s, final long offset) throws IOException {
      s.seek(offset);
      s.guess(MAGIC);
      final int blockSize = s.readInt();
      final int keySize = s.readInt();
      final int valSize = s.readInt();
      final long itemCount = s.readLong();
      s.readLong(); // reserved bytes
      final long rootOffset = s.tell();

      return new Header(s.order(), blockSize, keySize, valSize, itemCount, rootOffset);
    }
  }

  protected final Header header;

  public BPlusTree(final Header header) {
    this.header = Objects.requireNonNull(header);
  }

  /**
   * Recursively goes across tree, calling callback on the leaves.
   */
  public void traverse(final SeekableDataInput s, final Consumer<BPlusLeaf> consumer)
      throws IOException {
    final ByteOrder originalOrder = s.order();
    s.order(header.byteOrder);
    traverseRecursively(s, header.rootOffset, consumer);
    s.order(originalOrder);
  }

  private void traverseRecursively(final SeekableDataInput s,
                                   final long blockStart,
                                   final Consumer<BPlusLeaf> consumer)
      throws IOException {
    // Invariant: a stream is in Header.byteOrder.
    s.seek(blockStart);

    final boolean isLeaf = s.readBoolean();
    s.readBoolean(); // reserved
    final short childCount = s.readShort();

    final byte[] keyBuf = new byte[header.keySize];
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
        traverseRecursively(s, fileOffsets[i], consumer);
      }
    }
  }

  /**
   * Recursively traverses a B+ tree looking for a leaf corresponding
   * to {@code query}.
   */
  public Optional<BPlusLeaf> find(final SeekableDataInput s, final String query)
      throws IOException {
    if (query.length() > header.keySize) {
      return Optional.empty();
    }

    // FIXME: А зачем нужна проверка на размер поинтера? Пример (valSize != bpt->valSize)
    final ByteOrder originalOrder = s.order();
    s.order(header.byteOrder);

    // Trim query to 'keySize' because the spec. guarantees us
    // that all B+ tree nodes have a fixed-size key.
    final String trimmedQuery
        = query.substring(0, Math.min(query.length(), header.keySize));
    final Optional<BPlusLeaf> res
        = findRecursively(s, header.rootOffset, trimmedQuery);
    s.order(originalOrder);
    return res;
  }

  private Optional<BPlusLeaf> findRecursively(final SeekableDataInput s,
                                              final long blockStart,
                                              final String query)
      throws IOException {
    // Invariant: a stream is in Header.byteOrder.
    s.seek(blockStart);

    final boolean isLeaf = s.readBoolean();
    s.readBoolean(); // reserved
    final short childCount = s.readShort();

    final byte[] keyBuf = new byte[header.keySize];
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
      for (int i = 1; i < childCount; i++) {
        s.readFully(keyBuf);
        if (query.compareTo(new String(keyBuf)) < 0) {
          break;
        }

        fileOffset = s.readLong();
      }

      return findRecursively(s, fileOffset, query);
    }
  }
}
