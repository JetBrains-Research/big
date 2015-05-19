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

  void slAddHead(rTree listPt, rTree node)
/* Add new node to start of list.
 * Usage:
 *    slAddHead(&list, node);
 * where list and nodes are both pointers to structure
 * that begin with a next pointer.
 */
  {
    node.next = listPt;
    listPt = node;
  }

  lmBlock newBlock(lm lm, int reqSize)
/* Allocate a new block of at least reqSize */
  {
    int size = (reqSize > lm.blockSize ? reqSize : lm.blockSize);
    int fullSize = size + 32; //sizeof(struct lmBlock);
    lmBlock mb = new lmBlock(); // Потенциальная засада! Нужно выделить кусок памяти равным fullSize байт//needLargeZeroedMem(fullSize);

    // Поля free и end пока не используются
    //mb.free = (char *)(mb+1);
    //mb.end = ((char *)mb) + fullSize;

    mb.next = lm.blocks;
    lm.blocks = mb;
    return mb;
  }
  void slReverse(rTree listPt)
/* Reverse order of a list.
 * Usage:
 *    slReverse(&list);
 */
  {
    rTree newList = null;
    rTree el, next;

    next = listPt;
    while (next != null)
    {
      el = next;
      next = el.next;
      el.next = newList;
      newList = el;
    }
    listPt = newList;
  }

  rTree rTreeFromChromRangeArray( lm lm, int blockSize, int itemsPerSlot,
                                  bbiBoundsArray itemArray[], int itemSize, long itemCount,
                                  long endFileOffset,
                                  wrapObject retLevelCount) {

    if (itemCount == 0)
      return null;
    rTree el = null, list = null, tree = null;

/* Make first level above leaf. */
    long nextOffset = itemArray[0].offset;
    int oneSize = 0;
    for (int i=0; i<itemCount; i += oneSize)
    {

    /* Allocate element and put on list. */
      el = new rTree(); //lmAllocVar(lm, el);
      slAddHead(list, el);

    /* Fill out most of element from first item in element. */
      cirTreeRange key = itemArray[i].range;
      el.startChromIx = el.endChromIx = key.chromIx;
      el.startBase = key.start;
      el.endBase = key.end;
      el.startFileOffset = nextOffset;

      oneSize = 1;

      int j;
      for (j=i+1; j<itemCount; ++j) {
        nextOffset = itemArray[j].offset;//(*fetchOffset)(endItem, context);
        if (nextOffset != el.startFileOffset)
          break;
        else
          oneSize++;
      }
      if (j == itemCount) {
        nextOffset = endFileOffset;
      }

      el.endFileOffset = nextOffset;

    /* Expand area spanned to include all items in block. */
      for (j=1; j<oneSize; ++j)
      {
        key = itemArray[i+j].range;
        if (key.chromIx < el.startChromIx)
        {
          el.startChromIx = key.chromIx;
          el.startBase = key.start;
        }
        else if (key.chromIx == el.startChromIx)
        {
          if (key.start < el.startBase)
            el.startBase = key.start;
        }
        if (key.chromIx > el.endChromIx)
        {
          el.endChromIx = key.chromIx;
          el.endBase = key.end;
        }
        else if (key.chromIx == el.endChromIx)
        {
          if (key.end > el.endBase)
            el.endBase = key.end;
        }
      }
    }
    slReverse(list);

/* Now iterate through making more and more condensed versions until have just one. */
    int levelCount = 1;
    tree = list;
    while (tree.next != null || levelCount < 2)
    {
      list = null;
      int slotsUsed = blockSize;
      rTree parent = null, next;
      for (el = tree; el != null; el = next)
      {
        next = el.next;
        if (slotsUsed >= blockSize)
        {
          slotsUsed = 1;
          parent = new rTree(el); //lmCloneMem(lm, el, sizeof(*el));
          parent.children = el;
          el.parent = parent;
          el.next = null;
          slAddHead(list, parent);
        }
        else
        {
          ++slotsUsed;
          slAddHead(parent.children, el);
          el.parent = parent;
          if (el.startChromIx < parent.startChromIx)
          {
            parent.startChromIx = el.startChromIx;
            parent.startBase = el.startBase;
          }
          else if (el.startChromIx == parent.startChromIx)
          {
            if (el.startBase < parent.startBase)
              parent.startBase = el.startBase;
          }
          if (el.endChromIx > parent.endChromIx)
          {
            parent.endChromIx = el.endChromIx;
            parent.endBase = el.endBase;
          }
          else if (el.endChromIx == parent.endChromIx)
          {
            if (el.endBase > parent.endBase)
              parent.endBase = el.endBase;
          }
        }
      }

      slReverse(list);
      for (el = list; el != null; el = el.next)
        slReverse(el.children);
      tree = list;
      levelCount += 1;
    }
    retLevelCount.data = Integer.valueOf(levelCount);
    return tree;
  }

  lm lmInit(int blockSize)
/* Create a local memory pool. */
  {
    lm lm = new lm();

    int aliSize = 8;

    lm.blocks = null;
    if (blockSize <= 0)
      blockSize = (1<<14);    /* 16k default. */
    lm.blockSize = blockSize;
    lm.allignAdd = (aliSize-1);
    lm.allignMask = ~lm.allignAdd;
    newBlock(lm, blockSize);
    return lm;
  }

  private void cirTreeFileBulkIndexToOpenFile(bbiBoundsArray itemArray[], int itemSize, long itemCount,
                                              int blockSize, int itemsPerSlot,
                                              long endFileOffset, SeekableDataOutput writer) {
    wrapObject levelCount = new wrapObject();
    lm lm = lmInit(0);
    rTree tree = rTreeFromChromRangeArray(lm, blockSize, itemsPerSlot,
                                          itemArray, itemSize, itemCount, endFileOffset,
                                          levelCount);
    // TODO
    /*
    struct rTree dummyTree = {.startBase=0};
    if (tree == NULL)
      tree = &dummyTree;	// Work for empty files....
    bits32 magic = cirTreeSig;
    bits32 reserved = 0;
    writeOne(f, magic);
    writeOne(f, blockSize);
    writeOne(f, itemCount);
    writeOne(f, tree->startChromIx);
    writeOne(f, tree->startBase);
    writeOne(f, tree->endChromIx);
    writeOne(f, tree->endBase);
    writeOne(f, endFileOffset);
    writeOne(f, itemsPerSlot);
    writeOne(f, reserved);
    if (tree != &dummyTree)
      writeTreeToOpenFile(tree, blockSize, levelCount, f);
    lmCleanup(&lm);*/

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

    int blockSize = 4; // задается для B+ tree
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

    /* Write out primary data index. */
    long indexOffset = writer.tell();
    int itemSize = 24;
    cirTreeFileBulkIndexToOpenFile(boundsArray, itemSize, blockCount,
                                   blockSize, 1,
                                   indexOffset, writer);




  }
}
