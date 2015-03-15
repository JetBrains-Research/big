package org.jbb.big;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A common header for BigWIG and BigBED files.
 *
 * @author Sergey Zherevchuk
 */
public class BigHeader {

  public static final int BED_MAGIC = 0x8789f2eb;

  public static BigHeader parse(final SeekableStream s) throws Exception {
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

    if (reserved != 0) {
      throw new IllegalStateException("header extensions are not supported");
    }

    final List<ZoomLevel> zoomLevelList = new ArrayList<>();
    for (int i = 0; i < zoomLevels; i++) {
      zoomLevelList.add(new ZoomLevel(s.readInt(), s.readInt(),
                                      s.readLong(), s.readLong()));
    }

    final BptHeader bptHeader = BptHeader.read(s, chromTreeOffset);
    return new BigHeader(s.order(), s.filePath(), version, zoomLevels, chromTreeOffset,
                         unzoomedDataOffset, unzoomedIndexOffset, fieldCount, definedFieldCount,
                         asOffset, totalSummaryOffset, uncompressBufSize, zoomLevelList, bptHeader);
  }

  public final ByteOrder byteOrder;
  public final Path filePath;
  public final short version;
  public final short zoomLevels;
  public final long chromTreeOffset;
  public final long unzoomedDataOffset;  // FullDataOffset in table 5
  public final long unzoomedIndexOffset;
  public final short fieldCount;
  public final short definedFieldCount;
  public final long asOffset;
  public final long totalSummaryOffset;
  public final int uncompressBufSize;
  public final List<ZoomLevel> zoomLevelList;
  public final BptHeader bptHeader;

  public BigHeader(final ByteOrder byteOrder, final Path filePath, final short version,
                   final short zoomLevels, final long chromTreeOffset,
                   final long unzoomedDataOffset, final long unzoomedIndexOffset,
                   final short fieldCount, final short definedFieldCount, final long asOffset,
                   final long totalSummaryOffset, final int uncompressBufSize,
                   final List<ZoomLevel> zoomLevelList,
                   final BptHeader bptHeader) {
    this.byteOrder = byteOrder;
    this.filePath = filePath;
    this.version = version;
    this.zoomLevels = zoomLevels;
    this.chromTreeOffset = chromTreeOffset;
    this.unzoomedDataOffset = unzoomedDataOffset;
    this.unzoomedIndexOffset = unzoomedIndexOffset;
    this.fieldCount = fieldCount;
    this.definedFieldCount = definedFieldCount;
    this.asOffset = asOffset;
    this.totalSummaryOffset = totalSummaryOffset;
    this.uncompressBufSize = uncompressBufSize;
    this.zoomLevelList = zoomLevelList;
    this.bptHeader = bptHeader;
  }
}
