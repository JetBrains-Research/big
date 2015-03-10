package org.jbb.big;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author Sergey Zherevchuk
 */
public class BigBedToBed {

  public static void parse(final String path, final String chromName, final int chromStart,
                     final int chromEnd) throws Exception {

    final BigHeader bigHeader = BigHeader.parse(path);

    // FIXME: Open file channel again?
    try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
      FileChannel fc = raf.getChannel();
      int nodeBufferSize = 4;
      ByteBuffer nodeBuffer = ByteBuffer.allocate(nodeBufferSize);
      nodeBuffer.order(bigHeader.bptHeader.byteOrder);
      LinkedList<BptNodeLeaf> chromList = new LinkedList<>();
      rTraverseBPTree(fc, bigHeader.bptHeader, bigHeader.bptHeader.rootOffset, nodeBuffer,
                      chromList);

      // FIXME: выяснить почему R Tree Index присоединяется только в IntervalQuery.
      // Надо бы перенести в BigHeader?


      // traverse chrom linked list in reverse
      Iterator<BptNodeLeaf> iter = chromList.descendingIterator();
      while (iter.hasNext()) {
        BptNodeLeaf node = iter.next();

        LinkedList<Object> intervalList = bigBedIntervalQuery(bigHeader, chromName, chromStart,
                                                              chromEnd, 0);
        for (Object interval: intervalList) {
//          System.out.println( chromName, interval->start, interval->end);
        }
//        System.out.println("id: " + node.id + " size: " + node.size);
      }

    }
  }

  /**
   * Get data for interval.  Set maxItems to maximum number of items to return,
   * or to 0 for all items.
   */
   public static LinkedList<Object> bigBedIntervalQuery(BigHeader bigHeader, final String chromName,
                                               final int chromStart, final int chromEnd,
                                               final int maxItems) {
    LinkedList<Object> list = new LinkedList<>();
    int itemCount = 0;
    int chromId;

    LinkedList blockList = bbiOverlappingBlocks(bigHeader, chromName, chromStart, chromEnd,
                                                        maxItems);

    return list;
  }

  /* Fetch list of file blocks that contain items overlapping chromosome range. */
  public static LinkedList bbiOverlappingBlocks(BigHeader bigHeader, final String chromName,
                                                final int chromStart, final int chromEnd,
                                                final int maxItems) {
    LinkedList<Object> blockList = new LinkedList<>();

//    chromIdSizeHandleSwapped(bbi->isSwapped, &idSize);

    return blockList;
  }

  // Find value associated with key.  Return TRUE if it's found.
  public static boolean bptFileFind(BigHeader bigHeader, byte[] key, byte[] val) {
    return Boolean.TRUE; // FIXME
  }

  // Recursively go across tree, calling callback at leaves.
  public static void rTraverseBPTree(FileChannel fc, BptHeader bptHeader,
                               final long blockStart, ByteBuffer nodeBuffer,
                               LinkedList<BptNodeLeaf> chromList) throws IOException {
    fc.position(blockStart);
    // read tree node format info
    nodeBuffer.clear();
    fc.read(nodeBuffer);
    nodeBuffer.flip();
    boolean isLeaf = (nodeBuffer.get() == 1);
    byte reserved = nodeBuffer.get();
    short childCount = nodeBuffer.getShort();

    // calculate buffer size
    int itemSize = bptHeader.keySize + bptHeader.valSize;
    int itemBufferSize = itemSize * Integer.BYTES * childCount;

    // read items
    ByteBuffer ib = ByteBuffer.allocate(itemBufferSize).order(bptHeader.byteOrder);
    fc.read(ib);
    ib.flip();
    byte[] keyBuf = new byte[bptHeader.keySize];
    byte[] valBuf = new byte[bptHeader.valSize];
    if (isLeaf) {
      for (int i = 0; i < childCount; ++i) {
        ib.get(keyBuf);
        ib.get(valBuf);
        chromNameCallback(chromList, keyBuf, valBuf, bptHeader.byteOrder);
      }
    } else {
      long fileOffsets[] = new long[childCount];
      for (int i = 0; i < childCount; ++i) {
        ib.get(keyBuf);
        fileOffsets[i] = ib.getLong();
      }
      // traverse call for child nodes
      for (int i = 0; i < childCount; ++i) {
        rTraverseBPTree(fc, bptHeader, fileOffsets[i], nodeBuffer, chromList);
      }
    }
  }

  // Callback that captures chromInfo from bPlusTree.
  public static void chromNameCallback(LinkedList<BptNodeLeaf> chromList, byte[] key,
                                       byte[] val, ByteOrder byteOrder) {
    ByteBuffer b = ByteBuffer.wrap(val).order(byteOrder);
    final int chromId = b.getInt();
    final int chromSize = b.getInt();
    chromList.addFirst(new BptNodeLeaf(new String(key), chromId, chromSize));
  }
}