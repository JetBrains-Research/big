package org.jbb.big;

import com.google.common.primitives.Ints;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A common header for BigWIG and BigBED files.
 *
 * @author Sergey Zherevchuk
 */
public class BigHeader {

  public static final int BED_MAGIC = 0x8789f2eb;
  public static final int COMMON_HEADER_SIZE = 64;

  public static BigHeader parse(final String path) throws Exception {
    try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
      FileChannel fc = raf.getChannel();

      ByteBuffer cb = ByteBuffer.allocate(COMMON_HEADER_SIZE);
      fc.read(cb);
      cb.flip();
      // Determines byte order from the first four bytes of the Big file.
      final byte[] b = new byte[4];
      cb.get(b);
      final int bigMagic = Ints.fromBytes(b[0], b[1], b[2], b[3]);
      ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
      if (bigMagic != BED_MAGIC) {
        final int littleMagic = Ints.fromBytes(b[3], b[2], b[1], b[0]);
        if (littleMagic != BED_MAGIC) {
          throw new IllegalStateException("bad signature");
        }
        byteOrder = ByteOrder.LITTLE_ENDIAN;
      }
      cb.order(byteOrder);

      // Get common headers
      final short version = cb.getShort();
      final short zoomLevels = cb.getShort();
      final long chromTreeOffset = cb.getLong();
      final long unzoomedDataOffset = cb.getLong(); // fullDataOffset in supplemental info
      final long unzoomedIndexOffset = cb.getLong();
      final short fieldCount = cb.getShort();
      final short definedFieldCount = cb.getShort();
      final long asOffset = cb.getLong();
      final long totalSummaryOffset = cb.getLong();
      final int uncompressBufSize = cb.getInt();
      final long extensionOffset = cb.getLong();

      // Read zoom headers
      int zoomBufferSize = 24 * zoomLevels;
      ByteBuffer zb = ByteBuffer.allocate(zoomBufferSize);
      fc.read(zb);
      zb.flip();
      zb.order(byteOrder);
      final List<Map<String, Object>> zoomLevelList = new ArrayList<>();
      for (int i = 0; i < zoomLevels; i++) {
        Map<String, Object> level = new HashMap<>();
        level.put("reductionLevel", zb.getInt());
        level.put("reserved", zb.getInt());
        level.put("dataOffset", zb.getLong());
        level.put("reductionLevel", zb.getLong());
        zoomLevelList.add(level);
      }

      // Deal with header extension if any.
      short extensionSize = -1;
      short extraIndexCount = -1;
      long extraIndexListOffset = -1;
      if (extensionOffset != 0) {
        // FIXME: как-то грамотно сделать их опциональными
        raf.seek(extensionOffset);
        ByteBuffer hb = ByteBuffer.allocate(zoomBufferSize);
        fc.read(hb);
        hb.flip();
        hb.order(byteOrder);
        extensionSize = hb.getShort();
        extraIndexCount = hb.getShort();
        extraIndexListOffset = hb.getLong();
        throw new IllegalStateException("Table 5, extensionOffset. Currently 0?");
      }

      // Attach B+ tree of chromosome names and ids.
      BptHeader bptHeader = BptHeader.parse(fc, chromTreeOffset);

      // Free resources
      fc.close();

      return new BigHeader(version, zoomLevels, chromTreeOffset, unzoomedDataOffset,
                           unzoomedIndexOffset, fieldCount, definedFieldCount, asOffset,
                           totalSummaryOffset, uncompressBufSize, extensionOffset,
                           extensionSize, extraIndexCount, extraIndexListOffset, zoomLevelList,
                           bptHeader);
    }
  }

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
  public final long extensionOffset;
  public final short extensionSize;
  public final short extraIndexCount;
  public final long extraIndexListOffset;
  public final List<Map<String, Object>> zoomLevelList;
  public final BptHeader bptHeader;

  public BigHeader(final short version, final short zoomLevels, final long chromTreeOffset,
                   final long unzoomedDataOffset, final long unzoomedIndexOffset,
                   final short fieldCount, final short definedFieldCount,
                   final long asOffset, final long totalSummaryOffset,
                   final int uncompressBufSize, final long extensionOffset,
                   final short extensionSize, final short extraIndexCount,
                   final long extraIndexListOffset, final List<Map<String, Object>> zoomLevelList,
                   final BptHeader bptHeader) {
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
    this.extensionOffset = extensionOffset;
    this.extensionSize = extensionSize;
    this.extraIndexCount = extraIndexCount;
    this.extraIndexListOffset = extraIndexListOffset;
    this.zoomLevelList = zoomLevelList;
    this.bptHeader = bptHeader;
  }
}
