package org.jbb.big;

import junit.framework.TestCase;

import java.nio.file.Path;

public class RTreeIndexTest extends TestCase {
  public void testParseHeader() throws Exception {
    final Path inputPath = Examples.get("example1.bb");
    final SeekableDataInput s = SeekableDataInput.of(inputPath);
    final long unzoomedIndexOffset = 192771;
    final RTreeIndex rti = RTreeIndex.read(s, unzoomedIndexOffset);
    assertEquals(rti.header.blockSize, 1024);
    assertEquals(rti.header.itemCount, 14810);
    assertEquals(rti.header.startChromIx, 0);
    assertEquals(rti.header.startBase, 9434178);
    assertEquals(rti.header.endChromIx, 0);
    assertEquals(rti.header.endBase, 48099781);
    assertEquals(rti.header.fileSize, 192771);
    assertEquals(rti.header.itemsPerSlot, 64);
    assertEquals(rti.header.rootOffset, 192819);
  }
}