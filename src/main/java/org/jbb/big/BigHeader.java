package org.jbb.big;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * A common header for BigWIG and BigBED files.
 *
 * @author Sergey Zherevchuk
 */
public class BigHeader {

  public static final int BED_MAGIC = 0x8789f2eb;

  public static BigHeader parse(final SeekableDataInput s) throws Exception {
    s.guess(BED_MAGIC);

    final short version = s.readShort();
    final short zoomLevels = s.readShort();
    final long chromTreeOffset = s.readLong();
    final long unzoomedDataOffset = s.readLong(); // fullDataOffset in supplemental info
    final long unzoomedIndexOffset = s.readLong();
    final short fieldCount = s.readShort();
    final short definedFieldCount = s.readShort();
    final long asOffset = s.readLong();
    final long totalSummaryOffset = s.readLong();
    final int uncompressBufSize = s.readInt();
    final long reserved = s.readLong();  // currently 0.

    if (uncompressBufSize > 0) {
      throw new IllegalStateException("data compression is not supported");
    }

    if (reserved != 0) {
      throw new IllegalStateException("header extensions are not supported");
    }

    final List<ZoomLevel> zoomLevelList = new ArrayList<>();
    for (int i = 0; i < zoomLevels; i++) {
      zoomLevelList.add(new ZoomLevel(s.readInt(), s.readInt(),
                                      s.readLong(), s.readLong()));
    }

    final BPlusTree bpt = BPlusTree.read(s, chromTreeOffset);
    final RTreeIndex rti = RTreeIndex.read(s, unzoomedIndexOffset);
    return new BigHeader(s.order(), version, zoomLevels,
                         unzoomedDataOffset, fieldCount, definedFieldCount,
                         asOffset, totalSummaryOffset, uncompressBufSize, zoomLevelList,
                         bpt, rti);
  }

  public final ByteOrder byteOrder;
  public final short version;
  public final short zoomLevels;
  public final long unzoomedDataOffset;  // FullDataOffset in table 5
  public final short fieldCount;
  public final short definedFieldCount;
  public final long asOffset;
  public final long totalSummaryOffset;
  public final int uncompressBufSize;
  public final List<ZoomLevel> zoomLevelList;
  public final BPlusTree bPlusTree;
  public final RTreeIndex rTree;

  public BigHeader(final ByteOrder byteOrder, final short version,
                   final short zoomLevels, final long unzoomedDataOffset,
                   final short fieldCount, final short definedFieldCount, final long asOffset,
                   final long totalSummaryOffset, final int uncompressBufSize,
                   final List<ZoomLevel> zoomLevelList,
                   final BPlusTree bPlusTree, final RTreeIndex rTree) {
    this.byteOrder = byteOrder;
    this.version = version;
    this.zoomLevels = zoomLevels;
    this.unzoomedDataOffset = unzoomedDataOffset;
    this.fieldCount = fieldCount;
    this.definedFieldCount = definedFieldCount;
    this.asOffset = asOffset;
    this.totalSummaryOffset = totalSummaryOffset;
    this.uncompressBufSize = uncompressBufSize;
    this.zoomLevelList = zoomLevelList;
    this.bPlusTree = bPlusTree;
    this.rTree = rTree;
  }
}
