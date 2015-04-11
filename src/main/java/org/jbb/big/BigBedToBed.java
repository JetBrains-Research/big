package org.jbb.big;

import com.google.common.base.Joiner;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
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
    try (BigBedFile bf = BigBedFile.parse(inputPath);
         BufferedWriter out = Files.newBufferedWriter(outputPath)) {
      int itemCount = 0;
      for (final BPlusLeaf node : bf.chromosomes()) {
        if (!chromName.isEmpty() && !node.key.equals(chromName)) {
          continue;
        }

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
        for (final BedData interval : bf.query(node.key, start, end, itemsLeft)) {
          out.write(Joiner.on('\t').join(node.key, interval.start, interval.end, interval.rest));
          out.write('\n');
          itemCount++;
        }
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