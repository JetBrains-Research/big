package org.jbb.big;

import com.google.common.math.IntMath;
import com.google.common.math.LongMath;

import junit.framework.TestCase;

import org.junit.Assert;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

public class SeekableDataOutputTest extends TestCase {
  private static final int NUM_ATTEMPTS = 1000;

  public void testBigEndian() throws IOException {
    for (int i = 0; i < NUM_ATTEMPTS; i++) {
      testReadWrite(ByteOrder.BIG_ENDIAN);
    }
  }

  public void testLittleEndian() throws IOException {
    for (int i = 0; i < NUM_ATTEMPTS; i++) {
      testReadWrite(ByteOrder.LITTLE_ENDIAN);
    }
  }

  public void testReadWrite(final ByteOrder byteOrder)
      throws IOException {
    final Random random = new Random();
    final short shortValue = (short) random.nextInt();
    final int unsignedShortValue = random.nextInt(IntMath.pow(2, 16));
    final int intValue = random.nextInt();
    final long unsignedIntValue = Math.abs(random.nextLong()) % LongMath.pow(2, 32);
    final long longValue = random.nextLong();
    final float floatValue = random.nextFloat();
    final double doubleValue = random.nextDouble();
    final String utfStringValue = "I'm a UTF \u0441\u0442\u0440\u043e\u043a\u0430";
    final String charsArrayValue = "I'm a char array";
    final char severalSingleChar = 'a';
    final int severalSingleCharCount = 3;

    final String chromosomeName = "chrom1";
    final int keySize = 12;

    final Path path = Files.createTempFile(byteOrder.toString(), ".bb");
    try {
      try (final SeekableDataOutput w = SeekableDataOutput.of(path, byteOrder)) {
        w.writeShort(shortValue);
        w.writeUnsignedShort(unsignedShortValue);
        w.writeInt(intValue);
        w.writeUnsignedInt(unsignedIntValue);
        w.writeLong(longValue);
        w.writeFloat(floatValue);
        w.writeDouble(doubleValue);
        w.writeUTF(utfStringValue);
        w.writeBytes(charsArrayValue);
        w.writeByte(severalSingleChar, severalSingleCharCount);
        w.writeBytes(chromosomeName, keySize);
      }

      try (final SeekableDataInput r = SeekableDataInput.of(path, byteOrder)) {
        assertEquals(shortValue, r.readShort());
        assertEquals(unsignedShortValue, r.readUnsignedShort());
        assertEquals(intValue, r.readInt());
        assertEquals(unsignedIntValue, r.readUnsignedInt());
        assertEquals(longValue, r.readLong());
        assertEquals(floatValue, r.readFloat());
        assertEquals(doubleValue, r.readDouble());
        assertEquals(utfStringValue, r.readUTF());

        final byte[] charsArrayValueReader = new byte[charsArrayValue.length()];
        r.readFully(charsArrayValueReader);
        Assert.assertArrayEquals(charsArrayValue.getBytes(), charsArrayValueReader);

        final byte[] charSingle = new byte[severalSingleCharCount];
        r.readFully(charSingle);
        for(final byte b:charSingle) {
          Assert.assertEquals(severalSingleChar, b);
        }

        final byte[] chromosomeNameExt = new byte[keySize];
        r.readFully(chromosomeNameExt);
        for (int i = 0; i < keySize; ++i) {
          if(i < chromosomeName.length())
            Assert.assertEquals(chromosomeName.charAt(i), chromosomeNameExt[i]);
          else
            Assert.assertEquals(0, chromosomeNameExt[i]);
        }
      }
    } finally {
      Files.deleteIfExists(path);
    }
  }
}

