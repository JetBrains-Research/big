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

  final static int indexSlotSize = 24;	/* Size of startChrom,startBase,endChrom,endBase,offset */
  final static int leafSlotSize = 32;       /* Size of startChrom,startBase,endChrom,endBase,offset,size */
  final static int nodeHeaderSize = 4;	/* Size of rTree node header. isLeaf,reserved,childCount. */

  public static void writeBlocks(final List<bbiChromUsage> usageList, final Path bedFilePath,
                                 final int itemsPerSlot, final bbiBoundsArray bounds[],
                                 final int sectionCount, final boolean doCompress,
                                 final SeekableDataOutput writer,
                                 final int resTryCount, final int resScales[], final int resSizes[],
                                 final int bedCount, final short fieldCount)
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
          /* Save stream to file, compressing if need be. */
          maxBlockSize = Math.max(maxBlockSize, stream.size());
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
          blockStartOffset = writer.tell();
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
        stream.writeChar((char) 0);

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

  static rTree rTreeFromChromRangeArray(final int blockSize,
                                        final bbiBoundsArray itemArray[],
                                        final long dataEndOffset,
                                        final wrapObject retLevelCount) {
    final int itemCount = itemArray.length;
    if (itemCount == 0) {
      return null;
    }

    rTree el;
    rTree list = null;
    rTree tree;

    /* Make first level above leaf. */
    long nextOffset = itemArray[0].offset;
    int oneSize;
    for (int i = 0; i < itemCount; i += oneSize) {

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
      for (j = i + 1; j < itemCount; ++j) {
        nextOffset = itemArray[j].offset;//(*fetchOffset)(endItem, context);
        if (nextOffset != el.startFileOffset) {
          break;
        } else {
          oneSize++;
        }
      }
      if (j == itemCount) {
        nextOffset = dataEndOffset;
      }

      el.endFileOffset = nextOffset;

    /* Expand area spanned to include all items in block. */
      for (j = 1; j < oneSize; ++j) {
        key = itemArray[i + j].range;
        if (key.chromIx < el.startChromIx) {
          el.startChromIx = key.chromIx;
          el.startBase = key.start;
        } else if (key.chromIx == el.startChromIx) {
          if (key.start < el.startBase) {
            el.startBase = key.start;
          }
        }
        if (key.chromIx > el.endChromIx) {
          el.endChromIx = key.chromIx;
          el.endBase = key.end;
        } else if (key.chromIx == el.endChromIx) {
          if (key.end > el.endBase) {
            el.endBase = key.end;
          }
        }
      }
    }
    list = slReverse(list);

    /* Now iterate through making more and more condensed versions until have just one. */
    int levelCount = 1;
    tree = list;
    while (tree.next != null || levelCount < 2) {
      list = null;
      int slotsUsed = blockSize;
      rTree parent = null;
      rTree next;
      for (el = tree; el != null; el = next) {
        next = el.next;
        if (slotsUsed >= blockSize) {
          slotsUsed = 1;
          parent = new rTree(el); //lmCloneMem(lm, el, sizeof(*el));
          parent.children = el;
          el.next = null;

          parent.next = list; //slAddHead(list, parent);
          list = parent;
        } else {
          ++slotsUsed;
          //slAddHead(parent.children, el);
          el.next = parent.children;
          parent.children = el;

          if (el.startChromIx < parent.startChromIx) {
            parent.startChromIx = el.startChromIx;
            parent.startBase = el.startBase;
          } else if (el.startChromIx == parent.startChromIx) {
            if (el.startBase < parent.startBase) {
              parent.startBase = el.startBase;
            }
          }
          if (el.endChromIx > parent.endChromIx) {
            parent.endChromIx = el.endChromIx;
            parent.endBase = el.endBase;
          } else if (el.endChromIx == parent.endChromIx) {
            if (el.endBase > parent.endBase) {
              parent.endBase = el.endBase;
            }
          }
        }
      }

      list = slReverse(list);
      for (el = list; el != null; el = el.next) {
        el.children = slReverse(el.children);
      }
      tree = list;
      levelCount++;
    }
    retLevelCount.data = levelCount;
    return tree;
  }

  /* Count up elements in list. */
  private static int slCount(final rTree list) {
    rTree pt = list;
    int len = 0;

    while (pt != null) {
      len++;
      pt = pt.next;
    }
    return len;
  }

  private static rTree slReverse(final rTree list) {
    rTree newList = null;

    rTree cur = list;
    while (cur != null) {
      final rTree prv = new rTree(cur);
      prv.next = newList;
      newList = prv;
      cur = cur.next;
    }
    return newList;
  }

  static long rWriteIndexLevel(final int blockSize, final int childNodeSize,
                               final rTree tree, final int curLevel, final int destLevel,
                               final long offsetOfFirstChild,
                               final SeekableDataOutput writer)
      throws IOException
/* Recursively write an index level, skipping levels below destLevel,
 * writing out destLevel. */ {
    rTree el;
    long offset = offsetOfFirstChild;
    if (curLevel == destLevel) {
      /* We've reached the right level, write out a node header */
      final short childCount = (short) slCount(tree.children);
      writer.writeByte(0);  // isLeaf.
      writer.writeByte(0);  // reserved.
      writer.writeShort(childCount);

      /* Write out elements of this node. */
      for (el = tree.children; el != null; el = el.next) {
        writer.writeInt(el.startChromIx);
        writer.writeInt(el.startBase);
        writer.writeInt(el.endChromIx);
        writer.writeInt(el.endBase);
        writer.writeLong(offset);
        offset += childNodeSize;
      }

      /* Write out zeroes for empty slots in node. */
      writer.writeByte(0, indexSlotSize * (blockSize - childCount));
    } else {
      /* Otherwise recurse on children. */
      for (el = tree.children; el != null; el = el.next) {
        offset = rWriteIndexLevel(blockSize, childNodeSize, el, curLevel + 1, destLevel,
                                  offset, writer);
      }
    }
    return offset;
  }

  static void rWriteLeaves(final int itemsPerSlot, final rTree tree,
                           final int curLevel, final int leafLevel,
                           final SeekableDataOutput writer)
      throws IOException
/* Write out leaf-level nodes. */ {
    if (curLevel == leafLevel) {
    /* We've reached the right level, write out a node header. */
      final short childCount = (short) slCount(tree.children);
      writer.writeByte(1);  // isLeaf.
      writer.writeByte(0);  // reserved.
      writer.writeShort(childCount);

    /* Write out elements of this node. */
      rTree el;
      for (el = tree.children; el != null; el = el.next) {
        writer.writeInt(el.startChromIx);
        writer.writeInt(el.startBase);
        writer.writeInt(el.endChromIx);
        writer.writeInt(el.endBase);
        writer.writeLong(el.startFileOffset);
        final long size = el.endFileOffset - el.startFileOffset;
        writer.writeLong(size);
      }

      /* Write out zeroes for empty slots in node. */
      writer.writeByte(0, indexSlotSize * (itemsPerSlot - childCount));
    } else {
    /* Otherwise recurse on children. */
      rTree el;
      for (el = tree.children; el != null; el = el.next) {
        rWriteLeaves(itemsPerSlot, el, curLevel + 1, leafLevel, writer);
      }
    }
  }
}
