package org.jbb.big;

import com.google.common.primitives.Chars;

import junit.framework.TestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

public class SeekableDataOutputTest extends TestCase {

  short shortValue;
  int unsignedShortValue;
  int intValue;
  long unsignedIntValue;
  long longValue;
  float floatValue;
  double doubleValue;
  final String utfStringValue = "I'm UTF строка";
  final String charsArrayValue = "I'm char array";

  public SeekableDataOutputTest() {
    final Random random = new Random();
    shortValue = (short)random.nextInt();
    unsignedShortValue = random.nextInt() & ((1 << 16) - 1);
    unsignedShortValue |= 1 << 15;
    intValue = random.nextInt();
    unsignedIntValue = random.nextLong() & ((1L << 32) - 1);
    unsignedIntValue |= 1L << 31;
    longValue = random.nextLong();
    floatValue = random.nextFloat();
    doubleValue = random.nextDouble();
  }

  private void writeData(final SeekableDataOutput writer) throws IOException {
    writer.writeShort(shortValue);
    writer.writeUnsignedShort(unsignedShortValue);
    writer.writeInt(intValue);
    writer.writeUnsignedInt(unsignedIntValue);
    writer.writeLong(longValue);
    writer.writeFloat(floatValue);
    writer.writeDouble(doubleValue);
    writer.writeUTF(utfStringValue);
    writer.writeBytes(charsArrayValue);
  }

  private void readData(final SeekableDataInput reader) throws IOException {
    assertEquals(shortValue, reader.readShort());
    assertEquals(unsignedShortValue, reader.readUnsignedShort());
    assertEquals(intValue, reader.readInt());
    assertEquals(unsignedIntValue, reader.readUnsignedInt());
    assertEquals(longValue, reader.readLong());
    assertEquals(floatValue, reader.readFloat());
    assertEquals(doubleValue, reader.readDouble());
    assertEquals(utfStringValue, reader.readUTF());

    final byte[] charsArrayValueReader = new byte[charsArrayValue.length()];
    reader.readFully(charsArrayValueReader);
    assertTrue(Arrays.equals(charsArrayValue.getBytes(), charsArrayValueReader));
  }

  public void testBigEndian() throws IOException {
    final Path path = Files.createTempFile("seekableBE", ".bed");
    try {
      final SeekableDataOutput writer = SeekableDataOutput.of(path, ByteOrder.BIG_ENDIAN);
      writeData(writer);
      final SeekableDataInput reader = SeekableDataInput.of(path, ByteOrder.BIG_ENDIAN);
      readData(reader);
    }
    finally {
      Files.deleteIfExists(path);
    }
  }

  public void testLittleEndian() throws IOException {
    final Path path = Files.createTempFile("seekableLE", ".bed");
    try {
      final SeekableDataOutput writer = SeekableDataOutput.of(path, ByteOrder.LITTLE_ENDIAN);
      writeData(writer);
      final SeekableDataInput reader = SeekableDataInput.of(path, ByteOrder.LITTLE_ENDIAN);
      readData(reader);
    }
    finally {
      Files.deleteIfExists(path);
    }
  }
}
