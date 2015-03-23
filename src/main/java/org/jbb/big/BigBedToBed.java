package org.jbb.big;

import com.google.common.collect.Lists;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author Sergey Zherevchuk
 */
public class BigBedToBed {

  /**
   * Main method to convert from BigBED to BED format
   * @param inputPath Path to source *.bb file
   * @param chromName If set restrict output to given chromosome
   * @param chromStart If set, restrict output to only that over start. Should be zero by default.
   * @param chromEnd If set, restict output to only that under end. Should be zero to restrict by
   *                 chromosome size
   * @param maxItems If set, restrict output to first N items
   * @throws Exception
   */
  public static void main(final Path inputPath, final Path outputPath,
                          final String chromName, final int chromStart,
                          final int chromEnd, final int maxItems) throws Exception {
    try (SeekableDataInput s = SeekableDataInput.of(inputPath);
         BufferedWriter out = Files.newBufferedWriter(outputPath)) {
      // Parse common headers
      final BigHeader bigHeader = BigHeader.parse(s);

      // Construct list of chromosomes from B+ tree
      final LinkedList<BPlusLeaf> chromList = new LinkedList<>();
      s.order(bigHeader.bptHeader.byteOrder);
      BPlusTree.traverse(s, bigHeader.bptHeader, chromList::add);
      final int itemCount = 0;

      final RTreeIndexHeader rtiHeader
          = RTreeIndexHeader.read(s, bigHeader.unzoomedIndexOffset);

      // Loop through chromList in reverse
      final Iterator<BPlusLeaf> iter = chromList.descendingIterator();
      while (iter.hasNext()) {
        final BPlusLeaf node = iter.next();
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

        final RTreeRange query = RTreeRange.of(node.id, start, end);
        final List<BedData> intervalList
            = bigBedIntervalQuery(s, bigHeader, rtiHeader, query, itemsLeft);

        // Write data to output file
        for (final BedData interval : intervalList) {
          out.write(node.key + "\t" + interval.start + "\t" +  interval.end + "\t" + interval.rest + "\n");
        }
//        System.out.println("id: " + node.id + " size: " + node.size);
      }
    }
  }

  /**
   * Get data for interval.
   * @param s Stream
   * @param bigHeader Common headers
   * @param rTreeIndexHeader Headers for R-tree index
   * @param query
   * @param maxItems Maximum number of items to return, or 0 for all items.
   * @return
   * @throws IOException
   */
  public static List<BedData> bigBedIntervalQuery(final SeekableDataInput s,
                                                  final BigHeader bigHeader,
                                                  final RTreeIndexHeader rTreeIndexHeader,
                                                  final RTreeRange query,
                                                  final int maxItems) throws IOException {
    final LinkedList<RTreeIndexLeaf> overlappingBlockList = new LinkedList<>();
    s.order(rTreeIndexHeader.byteOrder);
    RTreeIndex.rFindOverlappingBlocks(overlappingBlockList, s, 0,
                                      rTreeIndexHeader.rootOffset, query);
    s.order(bigHeader.byteOrder);
    Collections.reverse(overlappingBlockList);

    int itemCount = 0;
    final int chromIx = query.left.chromIx;
    final List<BedData> res = Lists.newLinkedList();
    for (final RTreeIndexLeaf node : overlappingBlockList) {
      s.seek(node.dataOffset);

      do {
        if (s.readInt() != chromIx) {
          throw new IllegalStateException();
        }
        final int start = s.readInt();
        final int end = s.readInt();
        byte ch;
        final StringBuilder sb = new StringBuilder();
        for (; ; ) {
          ch = s.readByte();
          if (ch == 0) {
            break;
          }

          sb.append(ch);
        }

        if (start < query.left.offset) {
          continue;
        }
        if (end > query.right.offset) {
          break;
        }

        itemCount++;
        // Limit number of items
        if (maxItems > 0 && itemCount > maxItems) {
          break;
        }

        res.add(new BedData(chromIx, start, end, sb.toString()));
      } while (s.tell() - node.dataOffset < node.dataSize);
    }

    return res;
  }

  /**
   *  Starting at list, find all items that don't have a gap between them and the previous item.
   *  Return at gap, or at end of list, returning pointers to the items before and after the gap.
   */
  public static HashMap<String, RTreeIndexLeaf> fileOffsetSizeFindGap(
      final ListIterator<RTreeIndexLeaf> iter) {
    final HashMap<String, RTreeIndexLeaf> gaps = new HashMap<>(2);
    RTreeIndexLeaf pt = iter.next();
    while (iter.hasNext()) {
      final RTreeIndexLeaf next = iter.next();
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