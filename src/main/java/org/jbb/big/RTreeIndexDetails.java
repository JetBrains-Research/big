package org.jbb.big;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

public class RTreeIndexDetails {


  final static int bbiMaxZoomLevels = 10;
  final static int bbiResIncrement = 4;

  final static int indexSlotSize = 24;	/* Size of startChrom,startBase,endChrom,endBase,offset */
  final static int leafSlotSize = 32;       /* Size of startChrom,startBase,endChrom,endBase,offset,size */
  final static int nodeHeaderSize = 4;	/* Size of rTree node header. isLeaf,reserved,childCount. */

  public static void writeBlocks(final List<bbiChromUsage> usageList, final Path bedFilePath,
                           final int itemsPerSlot, final bbiBoundsArray bounds[],
                           final int sectionCount, final boolean doCompress, final SeekableDataOutput writer,
                           final int resTryCount, final int resScales[], final int resSizes[],
                           final int bedCount, final short fieldCount, final wrapObject retMaxBlockSize)
      throws IOException {

    int maxBlockSize = 0;
    final Iterator<bbiChromUsage> usageIterator = usageList.iterator();
    bbiChromUsage usage = usageIterator.next();
    final int lastField = fieldCount - 1;
    int itemIx = 0, sectionIx = 0;
    long blockStartOffset = 0;
    int startPos = 0, endPos = 0;
    int chromId = 0;
    final myStream stream = new myStream();

    //ByteArrayOutputStream stream = new ByteArrayOutputStream();

    /* Will keep track of some things that help us determine how much to reduce. */
    final int resEnds[] = new int[resTryCount];
    int resTry;
    for (resTry = 0; resTry < resTryCount; ++resTry)
      resEnds[resTry] = 0;

    /* Will keep track of some things that help us determine how much to reduce. */
    boolean atEnd = false, sameChrom = false;
    int start = 0, end = 0;

    /* Help keep track of which beds are in current chunk so as to write out
 * namedChunks to eim if need be. */
    final long sectionStartIx = 0, sectionEndIx = 0;

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
            final String s = row[i];
            stream.writeString(s);
            stream.writeChar('\t');
          }
	/* Write last field and terminal zero */
          final String s = row[lastField];
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
      retMaxBlockSize.data = maxBlockSize;
    }
  }

  public static int bbiCalcResScalesAndSizes(final int aveSize, final int resScales[],
                                       final int resSizes[])

/* Fill in resScales with amount to zoom at each level, and zero out resSizes based
 * on average span. Returns the number of zoom levels we actually will use. */
  {
    int resTryCount = bbiMaxZoomLevels, resTry;
    final int resIncrement = bbiResIncrement;
    final int minZoom = 10;
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

  // void slAddHead(rTree listPt, rTree node)
/* Add new node to start of list.
 * Usage:
 *    slAddHead(&list, node);
 * where list and nodes are both pointers to structure
 * that begin with a next pointer.
 */
 /* {
    node.next = listPt;
    listPt = node;
  }*/

  private static rTree slReverse(final rTree list)
/* Reverse order of a list.
 * Usage:
 *    slReverse(&list);
 */
  {
    rTree newList = null;

    rTree cur = list;
    while (cur != null) {
      final rTree prv = new rTree(cur);
      prv.next = newList;
      newList = prv;
      cur = cur.next;
    }
    return newList;
    /*
    rTree newList = null;
    rTree el, next;

    next = list;
    while (next != null)
    {
      el = next;
      next = el.next;
      el.next = newList;
      newList = el;
    }

    return newList;*/
  }

  private static lmBlock newBlock(final lm lm, final int reqSize)
/* Allocate a new block of at least reqSize */
  {
    final int size = (reqSize > lm.blockSize ? reqSize : lm.blockSize);
    final int fullSize = size + 32; //sizeof(struct lmBlock);
    final lmBlock mb = new lmBlock(); // Потенциальная засада! Нужно выделить кусок памяти равным fullSize байт//needLargeZeroedMem(fullSize);

    // Поля free и end пока не используются
    //mb.free = (char *)(mb+1);
    //mb.end = ((char *)mb) + fullSize;

    //mb.next = lm.blocks;
    lm.blocks.add(mb);
    return mb;
  }

  private static rTree rTreeFromChromRangeArray( final lm lm, final int blockSize, final int itemsPerSlot,
                                  final bbiBoundsArray itemArray[], final int itemSize, final long itemCount,
                                  final long endFileOffset,
                                  final wrapObject retLevelCount) {

    if (itemCount == 0)
      return null;
    rTree el, list = null, tree;

/* Make first level above leaf. */
    long nextOffset = itemArray[0].offset;
    int oneSize;
    for (int i=0; i<itemCount; i += oneSize)
    {

    /* Allocate element and put on list. */
      el = new rTree(); //lmAllocVar(lm, el);

      //slAddHead(list, el);
      el.next = list;
      list = el;

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
    list = slReverse(list);
    // list = slReverse(list);

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

          parent.next = list; //slAddHead(list, parent);
          list = parent;
        }
        else
        {
          ++slotsUsed;
          //slAddHead(parent.children, el);
          el.next = parent.children;
          parent.children = el;

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

      list = slReverse(list);
      for (el = list; el != null; el = el.next)
        el.children = slReverse(el.children);
      tree = list;
      levelCount += 1;
    }
    retLevelCount.data = levelCount;
    return tree;
  }

  private static lm lmInit(int blockSize)
/* Create a local memory pool. */
  {
    final lm lm = new lm();

    final int aliSize = 8;

    //lm.blocks = null;
    if (blockSize <= 0)
      blockSize = (1<<14);    /* 16k default. */
    lm.blockSize = blockSize;
    lm.allignAdd = (aliSize-1);
    lm.allignMask = ~lm.allignAdd;
    newBlock(lm, blockSize);
    return lm;
  }

  private static void calcLevelSizes(final rTree tree, final int levelSizes[], final int level, final int maxLevel)
/* Recursively count sizes of levels and add to appropriate slots of levelSizes */
  {
    rTree el;
    for (el = tree; el != null; el = el.next)
    {
      levelSizes[level] += 1;
      if (level < maxLevel)
        calcLevelSizes(el.children, levelSizes, level+1, maxLevel);
    }
  }

  private static int indexNodeSize(final int blockSize)
/* Return size of an index node. */
  {
    return nodeHeaderSize + indexSlotSize * blockSize;
  }

  private static int leafNodeSize(final int blockSize)
/* Return size of a leaf node. */
  {
    return nodeHeaderSize + leafSlotSize * blockSize;
  }

  /* Count up elements in list. */
  private static int slCount(final rTree list)
  {
    rTree pt = list;
    int len = 0;

    while (pt != null)
    {
      len += 1;
      pt = pt.next;
    }
    return len;
  }

  private static long rWriteIndexLevel(final int blockSize, final int childNodeSize,
                                final rTree tree, final int curLevel, final int destLevel,
                                final long offsetOfFirstChild, final SeekableDataOutput writer)
      throws IOException
/* Recursively write an index level, skipping levels below destLevel,
 * writing out destLevel. */
  {
    rTree el;
    long offset = offsetOfFirstChild;
    if (curLevel == destLevel)
    {
    /* We've reached the right level, write out a node header */
      final byte reserved = 0;
      final byte isLeaf = 0;
      final short countOne = (short)slCount(tree.children);
      writer.writeByte(isLeaf);
      writer.writeByte(reserved);
      writer.writeShort((int)countOne);

    /* Write out elements of this node. */
      for (el = tree.children; el != null; el = el.next)
      {
        writer.writeInt(el.startChromIx);
        writer.writeInt(el.startBase);
        writer.writeInt(el.endChromIx);
        writer.writeInt(el.endBase);
        writer.writeLong(offset);
        offset += childNodeSize;
      }

    /* Write out zeroes for empty slots in node. */
      int i;
      for (i=countOne; i<blockSize; ++i)
        writer.writeByte((char) 0, indexSlotSize);
    }
    else
    {
    /* Otherwise recurse on children. */
      for (el = tree.children; el != null; el = el.next)
        offset = rWriteIndexLevel(blockSize, childNodeSize, el, curLevel+1, destLevel,
                                  offset, writer);
    }
    return offset;
  }
  private static void rWriteLeaves(final int itemsPerSlot, final int lNodeSize, final rTree tree, final int curLevel,
                            final int leafLevel, final SeekableDataOutput writer) throws IOException
/* Write out leaf-level nodes. */
  {
    if (curLevel == leafLevel)
    {
    /* We've reached the right level, write out a node header. */
      final byte reserved = 0;
      final byte isLeaf = 1;
      final short countOne = (short)slCount(tree.children);
      writer.writeByte(isLeaf);
      writer.writeByte(reserved);
      writer.writeShort(countOne);

    /* Write out elements of this node. */
      rTree el;
      for (el = tree.children; el != null; el = el.next)
      {
        writer.writeInt(el.startChromIx);
        writer.writeInt(el.startBase);
        writer.writeInt(el.endChromIx);
        writer.writeInt(el.endBase);
        writer.writeLong(el.startFileOffset);
        final long size = el.endFileOffset - el.startFileOffset;
        writer.writeLong(size);
      }

    /* Write out zeroes for empty slots in node. */
      int i;
      for (i=countOne; i<itemsPerSlot; ++i)
        writer.writeByte((char) 0, indexSlotSize);
    }
    else
    {
    /* Otherwise recurse on children. */
      rTree el;
      for (el = tree.children; el != null; el = el.next)
        rWriteLeaves(itemsPerSlot, lNodeSize, el, curLevel+1, leafLevel, writer);
    }
  }
  private static void writeLeaves(final int itemsPerSlot, final int lNodeSize, final rTree tree, final int leafLevel, final SeekableDataOutput writer)
      throws IOException
/* Write out leaf-level nodes. */
  {
    rWriteLeaves(itemsPerSlot, lNodeSize, tree, 0, leafLevel, writer);
  }
  private static void writeIndexLevel(final int blockSize, final int childNodeSize,
                               final rTree tree, final long offsetOfFirstChild, final int level, final SeekableDataOutput writer)
      throws IOException
/* Write out a non-leaf level nodes at given level. */
  {
    rWriteIndexLevel(blockSize, childNodeSize, tree, 0, level, offsetOfFirstChild, writer);
  }
  private static void writeTreeToOpenFile(final rTree tree, final int blockSize, final int levelCount, final SeekableDataOutput writer)
      throws IOException
/* Write out tree to a file that is open already - writing out index nodes from
 * highest to lowest level, and then leaf nodes. */
  {
/* Calculate sizes of each level. */
    int i;
    final int levelSizes[] = new int[levelCount];
//    for (i=0; i<levelCount; ++i)
//      levelSizes[i] = 0;
    calcLevelSizes(tree, levelSizes, 0, levelCount-1);

/* Calc offsets of each level. */
    final long levelOffsets[] = new long[levelCount];
    long offset = writer.tell();
    final long iNodeSize = indexNodeSize(blockSize);
    final long lNodeSize = leafNodeSize(blockSize);
    for (i = 0; i < levelCount; ++i)
    {
      levelOffsets[i] = offset;
      offset += levelSizes[i] * iNodeSize;
    }

/* Write out index levels. */
    final int finalLevel = levelCount-3;
    for (i=0; i<=finalLevel; ++i)
    {
      final long childNodeSize = (i==finalLevel ? lNodeSize : iNodeSize);
      writeIndexLevel(blockSize, (int)childNodeSize, tree,
                      levelOffsets[i+1], i, writer);
    }

/* Write out leaf level. */
    final int leafLevel = levelCount - 2;
    writeLeaves(blockSize, leafNodeSize(blockSize), tree, leafLevel, writer);
  }

  public static void cirTreeFileBulkIndexToOpenFile(final bbiBoundsArray itemArray[], final int itemSize, final long itemCount,
                                              final int blockSize, final int itemsPerSlot,
                                              final long endFileOffset, final SeekableDataOutput writer)
      throws IOException {
    final wrapObject levelCount = new wrapObject();
    final lm lm = lmInit(0);
    rTree tree = rTreeFromChromRangeArray(lm, blockSize, itemsPerSlot,
                                          itemArray, itemSize, itemCount, endFileOffset,
                                          levelCount);

    final rTree dummyTree = new rTree();
    dummyTree.startBase = 0; // struct rTree dummyTree = {.startBase=0};

    if (tree == null)
      tree = new rTree(dummyTree);	// Work for empty files....

    final int magic = 0x2468ACE0;
    final int reserved = 0;
    writer.writeInt(magic);
    writer.writeInt(blockSize);
    writer.writeLong(itemCount);
    writer.writeInt(tree.startChromIx);
    writer.writeInt(tree.startBase);
    writer.writeInt(tree.endChromIx);
    writer.writeInt(tree.endBase);
    writer.writeLong(endFileOffset);
    writer.writeInt(itemsPerSlot);
    writer.writeInt(reserved);

    if (!tree.equals(dummyTree)) {
      writeTreeToOpenFile(tree, blockSize, levelCount.toInt(), writer);
    }
  }
}
