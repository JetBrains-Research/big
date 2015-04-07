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

public class SeekableDataOutputTest extends TestCase {

  final short   shortValue = 123;
  final int     unsignedShortValue = 65534;
  final int     intValue = 123456789;
  final long    unsignedIntValue = 3147483648L;
  final long    longValue = 1234567890123L;
  final float   floatValue = 9876.654321F;
  final double  doubleValue = 45697645132.468;
  final String  utfStringValue = "I'm UTF строка";
  final String  charsArrayValue = "I'm char array";

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
    final short shortValueReader = reader.readShort();
    assertEquals(shortValue, shortValueReader);
    final int unsigedShortValueReader = reader.readUnsignedShort();
    assertEquals(unsignedShortValue, unsigedShortValueReader);
    final int intValueReader = reader.readInt();
    assertEquals(intValue, intValueReader);
    final long unsignedIntValueReader = reader.readUnsignedInt();
    assertEquals(unsignedIntValue, unsignedIntValueReader);
    final long longValueReader = reader.readLong();
    assertEquals(longValue, longValueReader);
    final float floatValueReader = reader.readFloat();
    assertEquals(floatValue, floatValueReader);
    final double doubleValueReader = reader.readDouble();
    assertEquals(doubleValue, doubleValueReader);
    final String utfStringReader = reader.readUTF();
    assertEquals(utfStringValue, utfStringReader);

    final byte charsArrayValueReader[] = new byte[charsArrayValue.length()];
    reader.readFully(charsArrayValueReader);
    assertTrue(Arrays.equals(charsArrayValue.getBytes(), charsArrayValueReader));
  }

  public void testBigEndian() throws IOException {
    final Path path = Files.createTempFile("seekableBI", ".bed");
    final SeekableDataOutput writer = SeekableDataOutput.of(path, ByteOrder.BIG_ENDIAN);
    writeData(writer);
    final SeekableDataInput reader = SeekableDataInput.of(path, ByteOrder.BIG_ENDIAN);
    readData(reader);
  }

  public void testLittleEndian() throws IOException {
    final Path path = Files.createTempFile("seekableLI", ".bed");
    final SeekableDataOutput writer = SeekableDataOutput.of(path, ByteOrder.LITTLE_ENDIAN);
    writeData(writer);
    final SeekableDataInput reader = SeekableDataInput.of(path, ByteOrder.LITTLE_ENDIAN);
    readData(reader);
  }
}
