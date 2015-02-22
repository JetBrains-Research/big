package org.jbb.big.bed;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.primitives.Ints;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Parse BigBed Header
 *
 * @author Sergey Zherevchuk
 */
public class BigHeader {

  public static final int MAGIC = 0x8789f2eb;

  public static BigHeader parse(final Path path) throws Exception {
    try (DataInputStreamWrapper<?> w = getDataInput(path)) {
      final DataInput stream = w.getStream();
      final short version = stream.readShort();
      final short zoomLevels = stream.readShort();
      final long chromTreeOffset = stream.readLong();
      final long unzoomedDataOffset = stream.readLong();
      final long unzoomedIndexOffset = stream.readLong();
      final short fieldCount = stream.readShort();
      final short definedFieldCount = stream.readShort();
      final long asOffset = stream.readLong();
      final long totalSummaryOffset = stream.readLong();
      final int uncompressBufSize = stream.readInt();
      final long extensionOffset = stream.readLong();

      BigHeader bigHeader
          = new BigHeader(version, zoomLevels, chromTreeOffset, unzoomedDataOffset,
                        unzoomedIndexOffset, fieldCount, definedFieldCount, asOffset,
                        totalSummaryOffset, uncompressBufSize, extensionOffset);
      return bigHeader;
    }
  }

  public final short version;
  public final short zoomLevels;
  public final long chromTreeOffset;
  public final long unzoomedDataOffset; // FullDataOffset in table 5
  public final long unzoomedIndexOffset;
  public final short fieldCount;
  public final short definedFieldCount;
  public final long asOffset;
  public final long totalSummaryOffset;
  public final int uncompressBufSize;

  public final long extensionOffset;

  public BigHeader(final short version, final short zoomLevels, final long chromTreeOffset,
                   final long unzoomedDataOffset, final long unzoomedIndexOffset,
                   final short fieldCount, final short definedFieldCount,
                   final long asOffset, final long totalSummaryOffset,
                   final int uncompressBufSize, final long extensionOffset) {
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
  }

  /**
   * Check byte order. Return appropriate DataInputStream.
   *
   * @param path Path to bbi-file
   * @return DataInputStream | LittleEndianDataInputStream
   */
  public static DataInputStreamWrapper<?> getDataInput(final Path path) throws IOException {
    InputStream inputStream = Files.newInputStream(path);
    DataInputStream bigEndianInput = new DataInputStream(inputStream);
    byte[] b = new byte[4];
    bigEndianInput.readFully(b);
    int magic = Ints.fromByteArray(b);
    if (magic != MAGIC) {
      magic = Ints.fromBytes(b[3], b[2], b[1], b[0]);
      if (magic != MAGIC) {
        throw new IllegalStateException("bad signature");
      }
      LittleEndianDataInputStream littleEndianInput
          = new LittleEndianDataInputStream(inputStream);

      return new DataInputStreamWrapper<>(littleEndianInput);
    }

    return new DataInputStreamWrapper<>(bigEndianInput);
  }


}
