package org.jbb.big;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import kotlin.Pair;

public class RTreeIndexDetails {
  public static List<Pair<ChromosomeInterval, Long>> writeBlocks(
      final List<bbiChromUsage> usageList, final Path bedPath,
      final int itemsPerSlot,
      final boolean doCompress,
      final SeekableDataOutput output,
      final int bedCount, final short fieldCount)
      throws IOException {
    if (doCompress) {
      throw new UnsupportedOperationException("block compression is not supported");
    }

    final List<Pair<ChromosomeInterval, Long>> res = Lists.newArrayList();
    final Iterator<bbiChromUsage> usageIterator = usageList.iterator();
    bbiChromUsage usage = usageIterator.next();
    int itemIx = 0;
    long blockStartOffset = 0;
    int startPos = 0, endPos = 0;
    int chromId = 0;

    boolean sameChrom = false;
    int start = 0, end = 0;

    final Iterator<BedData> it = BedFile.read(bedPath).iterator();
    String rest = "";
    String chrom;

    for (; ;) {
      final boolean atEnd = !it.hasNext();
      if (it.hasNext()) {
        final BedData entry = it.next();
        chrom = entry.getName();
        start = entry.getStart();
        end = entry.getEnd();
        rest = entry.getRest();

        sameChrom = chrom.equals(usage.name);
      }

      /* Check conditions that would end block and save block info and advance to next if need be. */
      if (atEnd || !sameChrom || itemIx >= itemsPerSlot) {
        /* Save info on existing block. */
        res.add(new Pair<>(new ChromosomeInterval(chromId, startPos, endPos),
                           blockStartOffset));

        itemIx = 0;
        if (atEnd) {
          break;
        }
      }

      /* Advance to next chromosome if need be and get chromosome id. */
      if (!sameChrom) {
        usage = usageIterator.next();
      }

      chromId = usage.id;

      /* At start of block we save a lot of info. */
      if (itemIx == 0) {
        blockStartOffset = output.tell();
        startPos = start;
        endPos = end;
      }
      /* Otherwise just update end. */
      {
        if (endPos < end) {
          endPos = end;
        }
        /* No need to update startPos since list is sorted. */
      }

      /* Write out data. */
      output.writeInt(chromId);
      output.writeInt(start);
      output.writeInt(end);

      if (fieldCount > 3) {
        /* Write last field and terminal zero */
        output.writeBytes(rest);
      }
      output.writeByte(0);

      itemIx++;
    }

    return res;
  }
}