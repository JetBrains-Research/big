package org.jbb.big;

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.io.ByteArrayOutputStream;
import java.util.Vector;

/**
 * Created by slipstak2 on 30.04.15.
 */
public class RTreeIndexWriterTest extends TestCase {

  final static int bbiMaxZoomLevels = 10;
  final static int bbiResIncrement = 4;
  // chromName to chromSize
  private Hashtable <String, Integer> bbiChromSizesFromFile(Path chromSizes) throws IOException {
    Hashtable<String, Integer> result = new Hashtable<>();
    try (final BufferedReader reader = Files.newBufferedReader(chromSizes)) {
      String buffer;
      while ((buffer = reader.readLine()) != null) {
        final String[] params = buffer.split("\t");
        result.put(params[0], Integer.parseInt(params[1]));
      }
    }
    return result;
  }

  private ArrayList<bbiChromUsage> bbiChromUsageFromBedFile(Path bedFilePath,
                                               Hashtable<String, Integer> chromSizesHash,
                                               wrapObject retMinDiff,
                                               wrapObject retAveSize,
                                               wrapObject retBedCount) throws IOException {
    int maxRowSize = 3;
    Hashtable<String, Integer> uniqHash = new Hashtable<>();
    ArrayList<bbiChromUsage> usageList = new ArrayList<bbiChromUsage>();
    bbiChromUsage usage = null;
    int lastStart = -1;
    int id = 0;
    long totalBases = 0, bedCount = 0;
    int minDiff = Integer.MAX_VALUE;
    try (BufferedReader reader = Files.newBufferedReader(bedFilePath)){
      String buffer;
      while((buffer = reader.readLine()) != null) {
        String[] row = buffer.split("\t");
        String chrom = row[0];
        int start = Integer.parseInt(row[1]);
        int end = Integer.parseInt(row[2]);
        if (start > end) {
          throw new IllegalStateException(String.format("end (%d) before start (%d)", end, start));
        }
        ++bedCount;
        totalBases += (end - start);
        if (usage == null || !usage.name.equals(chrom)) {
          if (uniqHash.get(chrom) != null) {
            throw new IllegalStateException(String.format("bed file is not sorted. Duplicate chrom(%s)", chrom));
          }
          uniqHash.put(chrom, 1);
          Integer chromSize = chromSizesHash.get(chrom);
          if (chromSize == null) {
            throw new IllegalStateException(String.format("%s is not found in chromosome sizes file", chrom));
          }
          usage = new bbiChromUsage(chrom, id++, chromSize);
          usageList.add(usage);
          lastStart = -1;
        }
        if (end > usage.size) {
          throw new IllegalStateException(String.format("End coordinate %d bigger than %s size", end, usage.name));
        }
        usage.itemCount += 1;
        if (lastStart >= 0)
        {
          int diff = start - lastStart;
          if (diff < minDiff)
          {
            if (diff < 0) {
              throw new IllegalStateException("bed file is not sorted");
            }
            minDiff = diff;
          }
        }
        lastStart = start;
      }

      retAveSize.data = Integer.valueOf((int)(totalBases / bedCount));
      retMinDiff.data = Integer.valueOf(minDiff);
      retBedCount.data = Integer.valueOf((int)bedCount);
    }
    finally {
      return usageList;
    }
  }

  private void writeBlocks(ArrayList<bbiChromUsage> usageList, Path bedFilePath,
                           int itemsPerSlot, bbiBoundsArray bounds[],
                           int sectionCount, boolean doCompress, SeekableDataOutput writer,
                           int resTryCount, int resScales[], int resSizes[],
                           int bedCount, short fieldCount, wrapObject retMaxBlockSize)
      throws IOException {

    int maxBlockSize = 0;
    Iterator<bbiChromUsage> usageIterator = usageList.iterator();
    bbiChromUsage usage = usageIterator.next();
    int lastField = fieldCount - 1;
    int itemIx = 0, sectionIx = 0;
    long blockStartOffset = 0;
    int startPos = 0, endPos = 0;
    int chromId = 0;
    myStream stream = new myStream();

    //ByteArrayOutputStream stream = new ByteArrayOutputStream();

    /* Will keep track of some things that help us determine how much to reduce. */
    int resEnds[] = new int[resTryCount];
    int resTry;
    for (resTry = 0; resTry < resTryCount; ++resTry)
      resEnds[resTry] = 0;

    /* Will keep track of some things that help us determine how much to reduce. */
    boolean atEnd = false, sameChrom = false;
    int start = 0, end = 0;

    /* Help keep track of which beds are in current chunk so as to write out
 * namedChunks to eim if need be. */
    long sectionStartIx = 0, sectionEndIx = 0;

    String line;
    try (BufferedReader reader = Files.newBufferedReader(bedFilePath)) {

      String row[] = null;
      String chrom;

      for (; ; ) {
      /* Get next line of input if any. */
        if ((line = reader.readLine()) != null) {
        /* Chop up line and make sure the word count is right. */
          int wordCount;
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
	/* Save stream to file, compressing if need be. */
          if (stream.size() > maxBlockSize)
            maxBlockSize = stream.size();
          if (doCompress) {
            throw new UnsupportedOperationException("Don't use compression =)");
          } else {
            writer.write(stream.toByteArray());
          }
          stream.reset(); // очистка потока

	/* Save info on existing block. */
          bounds[sectionIx].offset = blockStartOffset;
          bounds[sectionIx].range.chromIx = chromId;
          bounds[sectionIx].range.start = startPos;
          bounds[sectionIx].range.end = endPos;

          ++sectionIx;
          itemIx = 0;

          if (atEnd)
            break;
        }

      /* Advance to next chromosome if need be and get chromosome id. */
        if (!sameChrom) {
          usage = usageIterator.next();
          for (resTry = 0; resTry < resTryCount; ++resTry)
            resEnds[resTry] = 0;
        }

        chromId = usage.id;

    /* At start of block we save a lot of info. */
        if (itemIx == 0) {
          blockStartOffset = writer.tell();
          startPos = start;
          endPos = end;
        }
    /* Otherwise just update end. */
        {
          if (endPos < end)
            endPos = end;
	/* No need to update startPos since list is sorted. */
        }

    /* Write out data. */
        stream.writeInt(chromId);
        stream.writeInt(start);
        stream.writeInt(end);

        if (fieldCount > 3) {
	/* Write 3rd through next to last field and a tab separator. */
          for (int i = 3; i < lastField; ++i) {
            String s = row[i];
            stream.writeString(s);
            stream.writeChar('\t');
          }
	/* Write last field and terminal zero */
          String s = row[lastField];
          stream.writeString(s);
        }
        stream.writeChar((char)0);

        itemIx += 1;

    /* Do zoom counting. */
        for (resTry = 0; resTry < resTryCount; ++resTry) {
          int resEnd = resEnds[resTry];
          if (start >= resEnd) {
            resSizes[resTry] =  1;
            resEnds[resTry] = resEnd = start + resScales[resTry];
          }
          while (end > resEnd) {
            resSizes[resTry] = 1;
            resEnds[resTry] = resEnd = resEnd + resScales[resTry];
          }
        }

      }
      retMaxBlockSize.data = Integer.valueOf(maxBlockSize);
    }
  }
  private int bbiCountSectionsNeeded(ArrayList<bbiChromUsage> usageList, int itemsPerSlot) {
    int count = 0;
    for(bbiChromUsage usage: usageList) {
      int countOne = (usage.itemCount + itemsPerSlot - 1)/itemsPerSlot;
      count += countOne;
    }
    return count;
  }

  private int bbiCalcResScalesAndSizes(int aveSize, int resScales[],
                                       int resSizes[])

/* Fill in resScales with amount to zoom at each level, and zero out resSizes based
 * on average span. Returns the number of zoom levels we actually will use. */
  {
    int resTryCount = bbiMaxZoomLevels, resTry;
    int resIncrement = bbiResIncrement;
    int minZoom = 10;
    int res = aveSize;
    if (res < minZoom)
      res = minZoom;
    for (resTry = 0; resTry < resTryCount; ++resTry)
    {
      resSizes[resTry] = 0;
      resScales[resTry] = res;
      // if aveSize is large, then the initial value of res is large, and we
      // and we cannot do all 10 levels without overflowing res* integers and other related variables.
      if (res > 1000000000)
      {
        resTryCount = resTry + 1;
        break;
      }
      res *= resIncrement;
    }
    return resTryCount;
  }
  public void test0() throws URISyntaxException, IOException {
    Path chromSizes = Examples.get("f2.chrom.sizes");
    Path bedFilePath = Examples.get("bedExample01.txt");
    Path pathBigBedFile = Files.createTempFile("BPlusTree", ".bb");;

    Hashtable<String, Integer> chromSizesHash = bbiChromSizesFromFile(chromSizes);
    wrapObject minDiff = new wrapObject();
    wrapObject aveSize = new wrapObject(), bedCount = new wrapObject();
    ArrayList<bbiChromUsage> usageList = bbiChromUsageFromBedFile(bedFilePath, chromSizesHash,
                                                                  minDiff, aveSize, bedCount);

    int resScales[] = new int[bbiMaxZoomLevels];
    int resSizes[] = new int[bbiMaxZoomLevels];
    int resTryCount = bbiCalcResScalesAndSizes(aveSize.toInt(), resScales, resSizes);


    int itemsPerSlot = 3; // ibelyaev
    int blockCount = bbiCountSectionsNeeded(usageList, itemsPerSlot);
    bbiBoundsArray boundsArray[] = new bbiBoundsArray[blockCount];
    for (int i = 0; i < blockCount; i++ ) boundsArray[i] = new bbiBoundsArray();

    boolean doCompress = false;
    final SeekableDataOutput writer = SeekableDataOutput.of(pathBigBedFile, ByteOrder.BIG_ENDIAN);
    short fieldCount = 3;
    wrapObject maxBlockSize = new wrapObject();


    writeBlocks(usageList, bedFilePath, itemsPerSlot, boundsArray, blockCount, doCompress, writer,
        resTryCount, resScales, resSizes,
        bedCount.toInt(), fieldCount, maxBlockSize);




  }
}
