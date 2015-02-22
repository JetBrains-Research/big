package org.jbb.big.bed;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.primitives.Ints;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Created by pacahon on 21.02.15.
 */
public class BigHeader {

  public static final int SIGNATURE = 0x8789f2eb;

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

  public BigHeader(short version, short zoomLevels, long chromTreeOffset, long unzoomedDataOffset,
                   long unzoomedIndexOffset, short fieldCount, short definedFieldCount,
                   long asOffset, long totalSummaryOffset,
                   int uncompressBufSize, long extensionOffset) {
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

  public static BigHeader parse(final Path path) throws IOException {
    DataInputStreamWrapper<?> w = getDataInput(path);

    short version = w.getStream().readShort();
    short zoomLevels = w.getStream().readShort();
    long chromTreeOffset = w.getStream().readLong();
    long unzoomedDataOffset = w.getStream().readLong();
    long unzoomedIndexOffset = w.getStream().readLong();
    short fieldCount = w.getStream().readShort();
    short definedFieldCount = w.getStream().readShort();
    long asOffset = w.getStream().readLong();
    long totalSummaryOffset = w.getStream().readLong();
    int uncompressBufSize = w.getStream().readInt();
    long extensionOffset = w.getStream().readLong();

    w.getStream().close();

    BigHeader
        bigHeader =
        new BigHeader(version, zoomLevels, chromTreeOffset, unzoomedDataOffset, unzoomedIndexOffset,
                      fieldCount, definedFieldCount, asOffset, totalSummaryOffset,
                      uncompressBufSize, extensionOffset);
    return bigHeader;
  }

  /**
   * Check byte order
   *
   * @param path Path to bbi-file
   * @return DataInput
   */
  public static DataInputStreamWrapper<?> getDataInput(final Path path) throws IOException {
    DataInputStream bigEndianInput = new DataInputStream(Files.newInputStream(path));
    byte[] chunkMagic = new byte[4];
    bigEndianInput.read(chunkMagic);
    int magicInt = Ints.fromByteArray(chunkMagic);
    if (magicInt != SIGNATURE) {
      magicInt = Ints.fromBytes(chunkMagic[3], chunkMagic[2], chunkMagic[1], chunkMagic[0]);
      if (magicInt != SIGNATURE) {
        throw new IOException("Bad signature"); // what about custom exception here?
      }
      bigEndianInput.close();
      LittleEndianDataInputStream
          littleEndianInput =
          new LittleEndianDataInputStream(Files.newInputStream(path));
      littleEndianInput.read(chunkMagic);

      return new DataInputStreamWrapper<>(littleEndianInput);
    }

    return new DataInputStreamWrapper<>(bigEndianInput);
  }


}
