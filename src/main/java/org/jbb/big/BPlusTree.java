package org.jbb.big;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;

import java.io.IOException;
import java.math.RoundingMode;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.List;
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

  /**
   * Counts the number of levels in a B+ tree for a given number
   * of items with fixed block size.
   *
   * Given block size (4 in the example) a B+ tree is laid out as:
   *
   *   [01|05]                          index level 1
   *   [01|02|03|04]   [05|06|07|08]    leaf  level 0
   *     ^               ^
   *    these are called blocks
   *
   * Conceptually, each B+ tree node consists of a number of
   * slots each holding {@code blockSize^level} items. So the
   * total number of items in a node can be calculated as
   * {@code blockSize^level * blockSize}
   *
   * @param blockSize number of slots in a B+ tree node.
   * @param itemCount total number of leaves in a B+ tree
   * @return required number of levels.
   */
  @VisibleForTesting
  protected static int countLevels(final int blockSize, int itemCount) {
    int levels = 1;
    while (itemCount > blockSize) {
      itemCount = IntMath.divide(itemCount, blockSize, RoundingMode.CEILING);
      levels++;
    }

    return levels;
  }

  static void write(final SeekableDataOutput output, final int blockSize,
                    final List<BPlusItem> unsortedItems)
      throws IOException {
    final List<BPlusItem> items = Lists.newArrayList(unsortedItems);
    items.sort(Comparator.comparing(item -> item.key));

    final int itemCount = items.size();
    final int keySize = items.stream()
        .mapToInt(item -> item.key.length()).max().getAsInt();

    final Header header = new Header(output.order(), blockSize, keySize,
                                     itemCount, output.tell() + Header.BYTES);
    header.write(output);
    long indexOffset = header.rootOffset;

    // HEAVY COMPUTER SCIENCE CALCULATION!
    final int bytesInNodeHeader = 1 + 1 + Short.BYTES;
    final int bytesInIndexSlot = keySize + Long.BYTES;
    final int bytesInIndexBlock = bytesInNodeHeader + blockSize * bytesInIndexSlot;
    final int bytesInLeafSlot = keySize + header.valSize;
    final int bytesInLeafBlock = bytesInNodeHeader + blockSize * bytesInLeafSlot;

    // Write B+ tree levels top to bottom.
    final int levels = countLevels(blockSize, items.size());
    for (int level = levels - 1; level > 0; --level)  {
      final int itemsPerSlot = IntMath.pow(blockSize, level);
      final int itemsPerNode = itemsPerSlot * blockSize;
      final int nodeCount
          = IntMath.divide(itemCount, itemsPerNode, RoundingMode.CEILING);

      final long bytesInCurrentLevel = nodeCount * bytesInIndexBlock;
      final long bytesInNextLevelBlock
          = level == 1 ? bytesInLeafBlock : bytesInIndexBlock;
      indexOffset += bytesInCurrentLevel;
      long nextChild = indexOffset;
      for (int i = 0; i < itemCount; i += itemsPerNode) {
        final int childCount = Math.min(
            IntMath.divide(itemCount - i, itemsPerSlot, RoundingMode.CEILING),
            blockSize);

        output.writeByte(0b0);  // isLeaf.
        output.writeByte(0b0);  // reserved.
        output.writeShort(childCount);
        for (int j = i; j < Math.min(i + itemsPerNode, itemCount); j += itemsPerSlot) {
          final BPlusItem item = items.get(j);
          output.writeBytes(item.key, keySize);
          output.writeLong(nextChild);
          nextChild += bytesInNextLevelBlock;
        }

        output.writeByte(0, bytesInIndexSlot * (blockSize - childCount));
      }
    }

    // Now just write the leaves.
    for (int i = 0; i < itemCount; i += blockSize) {
      final int childCount = Math.min(itemCount - i, blockSize);

      output.writeByte(0b1);  // isLeaf.
      output.writeByte(0b0);  // reserved.
      output.writeShort(childCount);
      for (int j = 0; j < childCount; j++) {
        final BPlusItem item = items.get(i + j);
        output.writeBytes(item.key, keySize);
        output.writeInt(item.id);
        output.writeInt(item.size);
      }

      output.writeByte(0, bytesInLeafSlot * (blockSize - childCount));
    }
  }

  @VisibleForTesting
  protected static class Header {
    /** Number of bytes used for this header. */
    private static final int BYTES = Integer.BYTES * 4 + Long.BYTES * 2;
    /** Magic number used for determining {@link ByteOrder}. */
    private static final int MAGIC = 0x78ca8c91;

    protected final ByteOrder byteOrder;
    protected final int blockSize;
    protected final int keySize;
    protected final int valSize = Integer.BYTES * 2;  // (ID, Size)
    protected final long itemCount;
    protected final long rootOffset;

    Header(final ByteOrder byteOrder, final int blockSize, final int keySize,
           final long itemCount, final long rootOffset) {
      this.byteOrder = byteOrder;
      this.blockSize = blockSize;
      this.keySize = keySize;
      this.itemCount = itemCount;
      this.rootOffset = rootOffset;
    }

    static Header read(final SeekableDataInput input, final long offset)
        throws IOException {
      input.seek(offset);
      input.guess(MAGIC);
      final int blockSize = input.readInt();
      final int keySize = input.readInt();
      final int valSize = input.readInt();
      if (valSize != Integer.BYTES * 2) {
        throw new IllegalStateException("inconsistent value size: " + valSize);
      }

      final long itemCount = input.readLong();
      input.readLong(); // reserved bytes
      final long rootOffset = input.tell();

      return new Header(input.order(), blockSize, keySize, itemCount, rootOffset);
    }

    void write(final SeekableDataOutput output) throws IOException {
      output.writeInt(MAGIC);
      output.writeInt(blockSize);
      output.writeInt(keySize);
      output.writeInt(valSize);
      output.writeLong(itemCount);
      output.writeLong(0L);  // reserved
    }
  }

  protected final Header header;

  @VisibleForTesting
  BPlusTree(final Header header) {
    this.header = Objects.requireNonNull(header);
  }

  /**
   * Recursively goes across tree, calling callback on the leaves.
   */
  public void traverse(final SeekableDataInput s, final Consumer<BPlusItem> consumer)
      throws IOException {
    final ByteOrder originalOrder = s.order();
    s.order(header.byteOrder);
    traverseRecursively(s, header.rootOffset, consumer);
    s.order(originalOrder);
  }

  private void traverseRecursively(final SeekableDataInput input,
                                   final long blockStart,
                                   final Consumer<BPlusItem> consumer)
      throws IOException {
    // Invariant: a stream is in Header.byteOrder.
    input.seek(blockStart);

    final boolean isLeaf = input.readBoolean();
    input.readBoolean();  // reserved
    final short childCount = input.readShort();

    final byte[] keyBuf = new byte[header.keySize];
    if (isLeaf) {
      for (int i = 0; i < childCount; i++) {
        input.readFully(keyBuf);
        final int chromId = input.readInt();
        final int chromSize = input.readInt();
        consumer.accept(new BPlusItem(new String(keyBuf).trim(), chromId, chromSize));
      }
    } else {
      final long[] fileOffsets = new long[childCount];
      for (int i = 0; i < childCount; i++) {
        input.readFully(keyBuf);  // XXX why can we overwrite it?
        fileOffsets[i] = input.readLong();
      }

      for (int i = 0; i < childCount; i++) {
        traverseRecursively(input, fileOffsets[i], consumer);
      }
    }
  }

  /**
   * Recursively traverses a B+ tree looking for a leaf corresponding
   * to {@code query}.
   */
  public Optional<BPlusItem> find(final SeekableDataInput input, final String query)
      throws IOException {
    if (query.length() > header.keySize) {
      return Optional.empty();
    }

    final ByteOrder originalOrder = input.order();
    input.order(header.byteOrder);

    // Trim query to 'keySize' because the spec. guarantees us
    // that all B+ tree nodes have a fixed-size key.
    final String trimmedQuery
        = query.substring(0, Math.min(query.length(), header.keySize));
    final Optional<BPlusItem> res
        = findRecursively(input, header.rootOffset, trimmedQuery);
    input.order(originalOrder);
    return res;
  }

  private Optional<BPlusItem> findRecursively(final SeekableDataInput input,
                                              final long blockStart,
                                              final String query)
      throws IOException {
    // Invariant: a stream is in Header.byteOrder.
    input.seek(blockStart);

    final boolean isLeaf = input.readBoolean();
    input.readBoolean(); // reserved
    final short childCount = input.readShort();

    final byte[] keyBuf = new byte[header.keySize];
    if (isLeaf) {
      for (int i = 0; i < childCount; i++) {
        input.readFully(keyBuf);
        final int chromId = input.readInt();
        final int chromSize = input.readInt();

        final String key = new String(keyBuf).trim();
        if (query.equals(key)) {
          return Optional.of(new BPlusItem(key, chromId, chromSize));
        }
      }

      return Optional.empty();
    } else {
      input.readFully(keyBuf);
      long fileOffset = input.readLong();
      // vvv we loop from 1 because we've read the first child above.
      for (int i = 1; i < childCount; i++) {
        input.readFully(keyBuf);
        if (query.compareTo(new String(keyBuf).trim()) < 0) {
          break;
        }

        fileOffset = input.readLong();
      }

      return findRecursively(input, fileOffset, query);
    }
  }
}
