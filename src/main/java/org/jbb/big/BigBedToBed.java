package org.jbb.big;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
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
      bigHeader.bPlusTree.traverse(s, chromList::add);
      final int itemCount = 0;

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

        final RTreeInterval query = RTreeInterval.of(node.id, start, end);
        // Write data to output file
        for (final BedData interval : bigHeader.rTree.findOverlaps(s, query, itemsLeft)) {
          out.write(node.key + "\t" + interval.start + "\t" +  interval.end + "\t" + interval.rest + "\n");
        }
//        System.out.println("id: " + node.id + " size: " + node.size);
      }
    }
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