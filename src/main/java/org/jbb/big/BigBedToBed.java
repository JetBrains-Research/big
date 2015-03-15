package org.jbb.big;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * @author Sergey Zherevchuk
 */
public class BigBedToBed {

  /**
   * Main method to convert from BigBED to BED format
   * @param path Path to source *.bb file
   * @param chromName If set restrict output to given chromosome
   * @param chromStart If set, restrict output to only that over start. Should be zero by default.
   * @param chromEnd If set, restict output to only that under end. Should be zero to restrict by
   *                 chromosome size
   * @param maxItems If set, restrict output to first N items
   * @throws Exception
   */
  public static void main(final Path path, final String chromName, final int chromStart,
                           final int chromEnd, final int maxItems) throws Exception {
    try (SeekableStream s = SeekableStream.of(path)) {
      // Parse common headers
      final BigHeader bigHeader = BigHeader.parse(s);
      // FIXME: chromFind не используется сейчас, мб и не надо хранить filePath

      // Construct list of chromosomes from B+ tree
      final LinkedList<BptNodeLeaf> chromList = new LinkedList<>();
      s.order(bigHeader.bptHeader.byteOrder);
      Bpt.rTraverse(s, bigHeader.bptHeader, bigHeader.bptHeader.rootOffset, chromList);
      final int itemCount = 0;

      // FIXME: выяснить почему R Tree Index присоединяется только в IntervalQuery.
      // Надо бы перенести в BigHeader как опциональные заголовки?
      final RTreeIndexHeader rtiHeader
          = RTreeIndexHeader.read(s, bigHeader.unzoomedIndexOffset);

      // Loop through chromList in reverse
      final Iterator<BptNodeLeaf> iter = chromList.descendingIterator();
      while (iter.hasNext()) {
        final BptNodeLeaf node = iter.next();
        // Filter by chromosome key
        if (!chromName.isEmpty() && !node.key.equals(chromName)) {
          continue;
        }
        // Check items left
        int itemsLeft = 0; // zero - no limit
        if (maxItems != 0) {
          itemsLeft = maxItems - itemCount;
          if (itemsLeft <= 0) {
            break;
          }
        }
        // Set restrictions  for interval
        final int start = (chromStart != 0) ? chromStart : 0;
        final int end = (chromEnd != 0) ? chromEnd: node.size;

        final LinkedList<BedData> intervalList
            = bigBedIntervalQuery(s, bigHeader, rtiHeader, node.id, start, end, itemsLeft);
        // Write data to output file
        Collections.reverse(intervalList); // FIXME: или descendingIterator?
        for (final BedData interval : intervalList) {
          System.out.println(chromName + "\t" + interval.start + "\t" +  interval.end);
        }
        System.out.println("id: " + node.id + " size: " + node.size);
      }
    }
  }

  /**
   * Get data for interval.
   * @param s Stream
   * @param bigHeader Common headers
   * @param rTreeIndexHeader Headers for R-tree index
   * @param chromId Chromosome id from B+ tree
   * @param chromStart If set, restrict output to only that over start
   * @param chromEnd If set, restrict output to only that under end
   * @param maxItems Maximum number of items to return, or 0 for all items.
   * @return
   * @throws IOException
   */
  public static LinkedList<BedData> bigBedIntervalQuery(final SeekableStream s,
                                                       final BigHeader bigHeader,
                                                       final RTreeIndexHeader rTreeIndexHeader,
                                                       final int chromId, final int chromStart,
                                                       final int chromEnd,
                                                       final int maxItems) throws IOException {
    final LinkedList<BedData> list = new LinkedList<>();
    final int itemCount = 0;
    final LinkedList<RTreeIndexNodeLeaf> overlappingBlockList = new LinkedList<>();
    s.order(rTreeIndexHeader.byteOrder);
    RTreeIndex.rFindOverlappingBlocks(overlappingBlockList, s, 0,
                                      rTreeIndexHeader.rootOffset, chromId, chromStart, chromEnd);
    s.order(bigHeader.byteOrder);
    // TODO: Set up for uncompression optionally?
    final boolean uncompressBuf = false;
    Collections.reverse(overlappingBlockList);
    final ListIterator<RTreeIndexNodeLeaf> iter = overlappingBlockList.listIterator();
    while (iter.hasNext()) {
      // New iterator from current position
      final ListIterator<RTreeIndexNodeLeaf> i = overlappingBlockList.listIterator(iter.nextIndex());
      RTreeIndexNodeLeaf block = iter.next();
      // Find contigious blocks and read them into mergedBuf.
      final HashMap<String, RTreeIndexNodeLeaf> gaps = fileOffsetSizeFindGap(i);
      final long mergedOffset = block.dataOffset;
      final long mergedSize
          = gaps.get("before").dataOffset + gaps.get("before").dataSize - mergedOffset;
      s.seek(mergedOffset);
      // FIXME: кастанул в int!
      final byte[] mergedBuf = new byte[(int)mergedSize];
      s.readFully(mergedBuf, 0, (int)mergedSize);
      // Loop through individual blocks within merged section.
      while (iter.hasNext() && (
          gaps.containsKey("after") && block.hashCode() != gaps.get("after").hashCode()
      )) {
        if (uncompressBuf) {
          // FIXME: skipped
        } else {
          byte[] blockPt = mergedBuf;
//          final long blockEnd = blockPt + block.dataSize; // FIXME: как это???
        }
        block = iter.next();
      }

    }
    return list;
  }

  /**
   *  Starting at list, find all items that don't have a gap between them and the previous item.
   *  Return at gap, or at end of list, returning pointers to the items before and after the gap.
   */
  public static HashMap<String, RTreeIndexNodeLeaf> fileOffsetSizeFindGap(
      final ListIterator<RTreeIndexNodeLeaf> iter) {
    final HashMap<String, RTreeIndexNodeLeaf> gaps = new HashMap<>(2);
    RTreeIndexNodeLeaf pt = iter.next();
    while (iter.hasNext()) {
      final RTreeIndexNodeLeaf next = iter.next();
      if (next.dataOffset != pt.dataOffset + pt.dataSize) {
        gaps.put("before", pt);
        gaps.put("after", next);
        return gaps;
      }
      pt = next;
    }
    gaps.put("before", pt);
    return gaps;
  }





}