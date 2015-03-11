package org.jbb.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author Sergey Zherevchuk
 */
public class BigBedToBed {

  public static void parse(final Path path, final String chromName, final int chromStart,
                           final int chromEnd, final int maxItems) throws Exception {

    final BigHeader bigHeader = BigHeader.parse(path);

    // FIXME: Open file channel again?
    final LinkedList<BptNodeLeaf> chromList = new LinkedList<>();
    final RTreeIndexHeader rTreeIndexHeader;
    try (SeekableStream s = SeekableStream.of(bigHeader.filePath)) {
      s.order(bigHeader.bptHeader.byteOrder);
      rTraverseBPTree(s, bigHeader.bptHeader, bigHeader.bptHeader.rootOffset, chromList);

      // FIXME: выяснить почему R Tree Index присоединяется только в IntervalQuery.
      // Надо бы перенести в BigHeader как опциональные заголовки?
      rTreeIndexHeader = RTreeIndexHeader.read(s, bigHeader.unzoomedIndexOffset);
    }

    // traverse chrom linked list in reverse
    final Iterator<BptNodeLeaf> iter = chromList.descendingIterator();
    while (iter.hasNext()) {
      final BptNodeLeaf node = iter.next();

      final LinkedList<Object> intervalList = bigBedIntervalQuery(bigHeader, rTreeIndexHeader,
                                                                  chromName,
                                                                  chromStart, chromEnd, maxItems);
        for (final Object interval: intervalList) {
//          System.out.println( chromName, interval->start, interval->end);
        }
      System.out.println("id: " + node.id + " size: " + node.size);
    }

  }

  /**
   * Get data for interval.  Set maxItems to maximum number of items to return,
   * or to 0 for all items.
   */
  public static LinkedList<Object> bigBedIntervalQuery(final BigHeader bigHeader,
                                                       RTreeIndexHeader rTreeIndexHeader,
                                                       final String chromName,
                                                       final int chromStart, final int chromEnd,
                                                       final int maxItems) throws IOException {
    final LinkedList<Object> list = new LinkedList<>();
    final int itemCount = 0;
    final int chromId;

    final LinkedList<Object> blockList
        = bbiOverlappingBlocks(bigHeader, rTreeIndexHeader, chromName, chromStart, chromEnd,
                             maxItems);
    return list;
  }

  /* Fetch list of file blocks that contain items overlapping chromosome range. */
  // return fileOffsetSize?
  public static LinkedList<Object> bbiOverlappingBlocks(final BigHeader bigHeader,
                                                        RTreeIndexHeader rTreeIndexHeader,
                                                        final String chromName,
                                                        final int chromStart, final int chromEnd,
                                                        final int maxItems) throws IOException {
    final LinkedList<Object> blockList = new LinkedList<>();
    if (!bptFileFind(bigHeader.filePath, bigHeader.bptHeader, chromName)) {
      return blockList;
    }

//    chromIdSizeHandleSwapped(bbi->isSwapped, &idSize);

    return blockList;
  }

  // Find value associated with key.  Return TRUE if it's found.
  public static boolean bptFileFind(final Path filePath,
                                    final BptHeader bptHeader,
                                    final String chromName) throws IOException {
    if (chromName.length() > bptHeader.keySize) {
      return Boolean.FALSE;
    }
    // FIXME: А зачем нужна проверка на размер поинтера? Пример (valSize != bpt->valSize)
    final boolean chromNameFounded;
    try (SeekableStream s = SeekableStream.of(filePath)) {
      chromNameFounded = rFindChromName(s, bptHeader, bptHeader.rootOffset, chromName);
    }
    return chromNameFounded;
  }

  /* Find value corresponding to key.  If found copy value to memory pointed to by val and return
  * true. Otherwise return false. */
  public static boolean rFindChromName(final SeekableStream s, final BptHeader bptHeader,
                                       final long blockStart,
                                       final String chromName) throws IOException {
    s.seek(blockStart);
    // read node format
    final boolean isLeaf = s.readBoolean();
    final boolean reserved = s.readBoolean();
    final short childCount = s.readShort();

    final byte[] keyBuf = new byte[bptHeader.keySize];
    final byte[] valBuf = new byte[bptHeader.valSize];

    if (isLeaf) {
      // FIXME: что java хочет от этих циклов??? Это из-за return? Как обойти?
      for (int i = 0; i < childCount; ++i) {
        s.read(keyBuf);
        s.read(valBuf);
        if (Arrays.equals(chromName.getBytes(), keyBuf)) {
          return Boolean.TRUE;
        }
        return Boolean.FALSE;
      }
    } else {
      final long fileOffsets[] = new long[childCount];
      for (int i = 0; i < childCount; ++i) {
        s.read(keyBuf);
        fileOffsets[i] = s.readLong();
      }
      // traverse call for child nodes
      for (int i = 0; i < childCount; ++i) {
        return rFindChromName(s, bptHeader, fileOffsets[i], chromName);
      }
    }
    // TODO: почему пишет, что функция ничего не возвращает. Как быть?
    return Boolean.FALSE;
  }

  // Recursively go across tree, calling callback at leaves.
  public static void rTraverseBPTree(final SeekableStream s, final BptHeader bptHeader,
                                     final long blockStart,
                                     final LinkedList<BptNodeLeaf> chromList) throws IOException {
    s.seek(blockStart);
    // read node format
    final boolean isLeaf = s.readBoolean();
    final boolean reserved = s.readBoolean();
    final short childCount = s.readShort();
    System.out.println(childCount);

    // read items
    final byte[] keyBuf = new byte[bptHeader.keySize];
    final byte[] valBuf = new byte[bptHeader.valSize];
    if (isLeaf) {
      for (int i = 0; i < childCount; ++i) {
        s.read(keyBuf);
        s.read(valBuf);
        chromNameCallback(chromList, keyBuf, valBuf, bptHeader.byteOrder);
      }
    } else {
      final long fileOffsets[] = new long[childCount];
      for (int i = 0; i < childCount; ++i) {
        s.read(keyBuf);
        fileOffsets[i] = s.readLong();
      }
      // traverse call for child nodes
      for (int i = 0; i < childCount; ++i) {
        rTraverseBPTree(s, bptHeader, fileOffsets[i], chromList);
      }
    }
  }

  // Callback that captures chromInfo from bPlusTree.
  public static void chromNameCallback(final LinkedList<BptNodeLeaf> chromList, final byte[] key,
                                       final byte[] val, final ByteOrder byteOrder) {
    final ByteBuffer b = ByteBuffer.wrap(val).order(byteOrder);
    final int chromId = b.getInt();
    final int chromSize = b.getInt();
    chromList.addFirst(new BptNodeLeaf(new String(key), chromId, chromSize));
  }
}