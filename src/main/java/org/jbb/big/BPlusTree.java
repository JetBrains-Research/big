package org.jbb.big;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
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
    private static final int bptFileHeaderSize = 32;
    private static final int bptBlockHeaderSize = 4;

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

    static int bptCountLevels(final int maxBlockSize, int itemCount) {
      int levels = 1;
      while (itemCount > maxBlockSize) {
        itemCount = (itemCount + maxBlockSize - 1) / maxBlockSize;
        levels += 1;
      }
      return levels;
    }

    static void writeLeafLevel(final SeekableDataOutput s, final int blockSize, final ArrayList<BPlusLeaf> itemArray, final int itemCount,
                               final int keySize, final int valSize) throws IOException
    /* Write out leaf level blocks. */
    {
      final byte isLeaf = 1;
      final byte reserved = 0;
      int countOne;
      int countLeft = itemCount;
      for (int i=0; i<itemCount; i += countOne)
      {
        /* Write block header */
        if (countLeft > blockSize)
          countOne = blockSize;
        else
          countOne = countLeft;

        s.writeByte(isLeaf);
        s.writeByte(reserved);
        s.writeShort(countOne);

        /* Write out position in genome and in file for each item. */
        for (int j=0; j<countOne; ++j)
        {
          s.writeBytes(itemArray.get(i+j).key, keySize);
          s.writeInt(itemArray.get(i + j).id);
          s.writeInt(itemArray.get(i+j).size);
        }

        /* Pad out any unused bits of last block with zeroes. */
        final int slotSize = keySize + valSize;
        for (int j=countOne; j<blockSize; ++j)
          s.writeByte(0, slotSize);

        countLeft -= countOne;
      }
    }
    static long writeIndexLevel(final SeekableDataOutput s, final int blockSize, final ArrayList<BPlusLeaf> itemArray, final int itemCount,
                                  final long indexOffset, final int level,
                                  final int keySize, final int valSize) throws IOException
    /* Write out a non-leaf level. */
    {
/* Calculate number of nodes to write at this level. */
      final int slotSizePer = (int)Math.pow(blockSize, level);   // Number of items per slot in node
      final int nodeSizePer = slotSizePer * blockSize;  // Number of items per node
      final int nodeCount = (itemCount + nodeSizePer - 1)/nodeSizePer;


/* Calculate sizes and offsets. */
      final int bytesInIndexBlock = (bptBlockHeaderSize + blockSize * (keySize + 8)); // 8=sizeof(long)
      final int bytesInLeafBlock = (bptBlockHeaderSize + blockSize * (keySize+valSize));
      final long bytesInNextLevelBlock = (level == 1 ? bytesInLeafBlock : bytesInIndexBlock);
      final long levelSize = nodeCount * bytesInIndexBlock;
      final long endLevel = indexOffset + levelSize;
      long nextChild = endLevel;


      final byte isLeaf = 0;
      final byte reserved = 0;

      for (int i=0; i<itemCount; i += nodeSizePer)
      {
        /* Calculate size of this block */
        int countOne = (itemCount - i + slotSizePer - 1)/slotSizePer;
        if (countOne > blockSize)
          countOne = blockSize;
        final int shortCountOne = countOne;

        /* Write block header. */
        s.writeByte(isLeaf);
        s.writeByte(reserved);
        s.writeShort(shortCountOne);

        /* Write out the slots that are used one by one, and do sanity check. */
        int slotsUsed = 0;
        int endIx = i + nodeSizePer;
        if (endIx > itemCount)
          endIx = itemCount;
        for (int j=i; j<endIx; j += slotSizePer)
        {
          s.writeBytes(itemArray.get(j).key, keySize);
          s.writeLong(nextChild);
          nextChild += bytesInNextLevelBlock;
          ++slotsUsed;
        }

        /* Write out empty slots as all zero. */
        final int slotSize = keySize + 8;//8 = sizeof(bits64);
        for (int j=countOne; j<blockSize; ++j)
          s.writeByte(0, slotSize);
      }
      return endLevel;
    }
    static void write(final SeekableDataOutput s, final ArrayList<BPlusLeaf> itemArray,
                      final int blockSize) throws IOException {
      final int itemCount = itemArray.size();
      int keySize = 0;
      for(final BPlusLeaf item :itemArray) keySize = Math.max(keySize, item.key.length());
      final int valSize = 8;

      s.writeInt(MAGIC);
      s.writeInt(blockSize);
      s.writeInt(keySize);
      s.writeInt(valSize);
      s.writeLong(itemCount);
      s.writeLong(0L); // reserved
      long indexOffset = s.tell();

      final int levels = bptCountLevels(blockSize, itemCount);
      for (int i = levels-1; i > 0; --i)  {
        final long endLevelOffset = writeIndexLevel(s, blockSize, itemArray, itemCount, indexOffset,
                                                i, keySize, valSize);
        indexOffset = s.tell();
      }
      writeLeafLevel(s, blockSize, itemArray, itemCount, keySize, valSize);

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
        consumer.accept(new BPlusLeaf(new String(keyBuf).trim(), chromId, chromSize));
      }
    } else {
      final long[] fileOffsets = new long[childCount];
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

        final String key = (new String(keyBuf)).trim();
        if (query.equals(key)) {
          return Optional.of(new BPlusLeaf(key, chromId, chromSize));
        }
      }

      return Optional.empty();
    } else {
      s.readFully(keyBuf);
      long fileOffset = s.readLong();
      // vvv we loop from 1 because we've read the first child above.
      for (int i = 1; i < childCount; i++) {
        s.readFully(keyBuf);
        if (query.compareTo(new String(keyBuf).trim()) < 0) {
          break;
        }

        fileOffset = s.readLong();
      }

      return findRecursively(s, fileOffset, query);
    }
  }
}
