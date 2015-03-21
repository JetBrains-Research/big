package org.jbb.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;


/**
 * B+ tree class
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
public class Bpt {

  /**
   * Recursively go across tree, calling callback at leaves.
   * See tables 9-11 in Supplemental
   */
  public static void rTraverse(final SeekableDataInput s, final BptHeader bptHeader,
                               final long blockStart,
                               final List<BptNodeLeaf> chromList) throws IOException {
    s.seek(blockStart);
    // read node format
    final boolean isLeaf = s.readBoolean();
    s.readBoolean(); // reserved
    final short childCount = s.readShort();

    // read items
    final byte[] keyBuf = new byte[bptHeader.keySize];
    final byte[] valBuf = new byte[bptHeader.valSize];
    if (isLeaf) {
      for (int i = 0; i < childCount; i++) {
        s.readFully(keyBuf);
        s.readFully(valBuf);
        addChromInfoCallback(chromList, keyBuf, valBuf, bptHeader.byteOrder);
      }
    } else {
      final long fileOffsets[] = new long[childCount];
      for (int i = 0; i < childCount; i++) {
        s.readFully(keyBuf);
        fileOffsets[i] = s.readLong();
      }
      // traverse call for child nodes
      for (int i = 0; i < childCount; i++) {
        rTraverse(s, bptHeader, fileOffsets[i], chromList);
      }
    }
  }

  /**
   * Callback that captures chromInfo from bPlusTree and add to head of chromList.
   */
  public static void addChromInfoCallback(final List<BptNodeLeaf> chromList, final byte[] key,
                                          final byte[] val, final ByteOrder byteOrder) {
    final ByteBuffer b = ByteBuffer.wrap(val).order(byteOrder);
    final int chromId = b.getInt();
    final int chromSize = b.getInt();
    chromList.add(new BptNodeLeaf(new String(key), chromId, chromSize));
  }

  /**
   * Find value associated with key. Return BptNodeLeaf if it's found.
   */
  public static Optional<BptNodeLeaf> chromFind(final Path filePath,
                                                   final BptHeader bptHeader,
                                                   final String chromName) throws IOException {
    if (chromName.length() > bptHeader.keySize) {
      return Optional.empty();
    }
    // TODO: Кажется, нужно обрезать chromName по keySize. В таблице 10 речь про first keySize
    // characters of chromosome name
    // FIXME: А зачем нужна проверка на размер поинтера? Пример (valSize != bpt->valSize)
    // Интересно, как на дереве это отражается
    final Optional<BptNodeLeaf> bptNodeLeaf;
    try (SeekableDataInput s = SeekableDataInput.of(filePath)) {
      s.order(bptHeader.byteOrder);
      bptNodeLeaf = rFindChromByName(s, bptHeader, bptHeader.rootOffset, chromName);
    }
    return bptNodeLeaf;
  }

  /**
   * Find value corresponding to key.
   */
  private static Optional<BptNodeLeaf> rFindChromByName(final SeekableDataInput s,
                                                        final BptHeader bptHeader,
                                                        final long blockStart,
                                                        final String chromName) throws IOException {
    s.seek(blockStart);
    // Read node format
    final boolean isLeaf = s.readBoolean();
    s.readBoolean(); // reserved
    final short childCount = s.readShort();

    final byte[] keyBuf = new byte[bptHeader.keySize];
    final byte[] valBuf = new byte[bptHeader.valSize];
    if (isLeaf) {
      for (int i = 0; i < childCount; i++) {
        s.readFully(keyBuf);
        s.readFully(valBuf);
        if (chromName.equals(new String(keyBuf))) {
          final ByteBuffer b = ByteBuffer.wrap(valBuf).order(bptHeader.byteOrder);
          final int chromId = b.getInt();
          final int chromSize = b.getInt();
          return Optional.of(new BptNodeLeaf(new String(keyBuf), chromId, chromSize));
        }
      }
      return Optional.empty();
    } else {
      s.readFully(keyBuf);
      long fileOffset = s.readLong();
      for (int i = 0; i < childCount; i++) {
        s.readFully(keyBuf);
        if (chromName.compareTo(new String(keyBuf)) < 0) {
          break;
        }
        fileOffset = s.readLong();
      }
      return rFindChromByName(s, bptHeader, fileOffset, chromName);
    }
  }
}
