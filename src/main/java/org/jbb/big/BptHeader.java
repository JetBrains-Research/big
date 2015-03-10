package org.jbb.big;

import com.google.common.primitives.Ints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * Chromosome B+ tree headers for bigBED.
 *
 * @author Sergey Zherevchuk
 */
public class BptHeader {

  public static final int MAGIC = 0x78ca8c91;
  public static final int HEADER_SIZE = 32;

  public ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
  public final int blockSize;
  public final int keySize;
  public final int valSize;
  public final long itemCount;
  public final long rootOffset;

  public BptHeader(ByteOrder byteOrder, final int blockSize, final  int keySize,
                   final int valSize, final long itemCount, final long rootOffset) {
    this.byteOrder = byteOrder;
    this.blockSize = blockSize;
    this.keySize = keySize;
    this.valSize = valSize;
    this.itemCount = itemCount;
    this.rootOffset = rootOffset;
  }

  // Attach B+ tree of chromosome names and ids.
  public static BptHeader parse(FileChannel fc, final long chromTreeOffset) throws IOException {
    fc.position(chromTreeOffset);
    ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
    final byte[] b = new byte[4];
    fc.read(buffer);
    buffer.flip();
    // Determines byte order for B+ tree
    buffer.get(b); // 4 bytes array
    final int bigMagic = Ints.fromBytes(b[0], b[1], b[2], b[3]);
    ByteOrder bptByteOrder = ByteOrder.BIG_ENDIAN;
    if (bigMagic != MAGIC) {
      final int littleMagic = Ints.fromBytes(b[3], b[2], b[1], b[0]);
      if (littleMagic != MAGIC) {
        throw new IllegalStateException("bad signature B+ tree");
      }
      bptByteOrder = ByteOrder.LITTLE_ENDIAN;
    }
    buffer.order(bptByteOrder);
    final int blockSize = buffer.getInt();
    final int keySize = buffer.getInt();
    final int valSize = buffer.getInt();
    final long itemCount = buffer.getLong();
    buffer.getLong(); // reserved bytes
    final long rootOffset = fc.position();

    return new BptHeader(bptByteOrder, blockSize, keySize, valSize,
                                        itemCount, rootOffset);
  }
}
