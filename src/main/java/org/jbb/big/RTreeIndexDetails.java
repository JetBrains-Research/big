package org.jbb.big;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RTreeIndexDetails {
  final static int bbiMaxZoomLevels = 10;
  final static int bbiResIncrement = 4;

  public static void writeBlocks(final List<bbiChromUsage> usageList, final Path bedFilePath,
                                 final int itemsPerSlot, final bbiBoundsArray bounds[],
                                 final int sectionCount, final boolean doCompress,
                                 final SeekableDataOutput output,
                                 final int resTryCount, final int resScales[], final int resSizes[],
                                 final int bedCount, final short fieldCount)
      throws IOException {
    if (doCompress) {
      throw new UnsupportedOperationException("block compression is not supported");
    }

    final Iterator<bbiChromUsage> usageIterator = usageList.iterator();
    bbiChromUsage usage = usageIterator.next();
    final int lastField = fieldCount - 1;
    int itemIx = 0, sectionIx = 0;
    long blockStartOffset = 0;
    int startPos = 0, endPos = 0;
    int chromId = 0;

    /* Will keep track of some things that help us determine how much to reduce. */
    final int resEnds[] = new int[resTryCount];

    /* Will keep track of some things that help us determine how much to reduce. */
    boolean atEnd = false, sameChrom = false;
    int start = 0, end = 0;

    /* Help keep track of which beds are in current chunk so as to write out
    * namedChunks to eim if need be. */
    final long sectionStartIx = 0, sectionEndIx = 0;

    try (BufferedReader reader = Files.newBufferedReader(bedFilePath)) {
      String line;
      String row[] = null;
      String chrom;

      for (; ;) {
        /* Get next line of input if any. */
        if ((line = reader.readLine()) != null) {
          /* Chop up line and make sure the word count is right. */
          row = line.split("\t");

          chrom = row[0];
          start = Integer.parseInt(row[1]);
          end = Integer.parseInt(row[2]);

          sameChrom = chrom.equals(usage.name);
        } else {  /* No next line */
          atEnd = true;
        }

        /* Check conditions that would end block and save block info and advance to next if need be. */
        if (atEnd || !sameChrom || itemIx >= itemsPerSlot) {
	  /* Save info on existing block. */
          bounds[sectionIx].offset = blockStartOffset;
          bounds[sectionIx].range.chromIx = chromId;
          bounds[sectionIx].range.start = startPos;
          bounds[sectionIx].range.end = endPos;

          ++sectionIx;
          itemIx = 0;

          if (atEnd) {
            break;
          }
        }

        /* Advance to next chromosome if need be and get chromosome id. */
        if (!sameChrom) {
          usage = usageIterator.next();
          Arrays.fill(resEnds, 0);
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
	  /* Write 3rd through next to last field and a tab separator. */
          for (int i = 3; i < lastField; ++i) {
            output.writeBytes(row[i]);
            output.writeByte('\t');
          }
	  /* Write last field and terminal zero */
          output.writeBytes(row[lastField]);
        }
        output.writeByte(0);

        itemIx++;

        /* Do zoom counting. */
        for (int resTry = 0; resTry < resTryCount; ++resTry) {
          int resEnd = resEnds[resTry];
          if (start >= resEnd) {
            resSizes[resTry] = 1;
            resEnds[resTry] = resEnd = start + resScales[resTry];
          }
          while (end > resEnd) {
            resSizes[resTry] = 1;
            resEnds[resTry] = resEnd = resEnd + resScales[resTry];
          }
        }

      }
    }
  }

  public static int bbiCalcResScalesAndSizes(final int aveSize, final int resScales[],
                                             final int resSizes[])

/* Fill in resScales with amount to zoom at each level, and zero out resSizes based
 * on average span. Returns the number of zoom levels we actually will use. */ {
    int resTryCount = bbiMaxZoomLevels, resTry;
    final int resIncrement = bbiResIncrement;
    final int minZoom = 10;
    int res = aveSize;
    if (res < minZoom) {
      res = minZoom;
    }
    for (resTry = 0; resTry < resTryCount; ++resTry) {
      resSizes[resTry] = 0;
      resScales[resTry] = res;
      // if aveSize is large, then the initial value of res is large, and we
      // and we cannot do all 10 levels without overflowing res* integers and other related variables.
      if (res > 1000000000) {
        resTryCount = resTry + 1;
        break;
      }
      res *= resIncrement;
    }
    return resTryCount;
  }
}