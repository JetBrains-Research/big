package org.jbb.big;

import junit.framework.TestCase;

public class BigFileTest extends TestCase {
  public void testParseHeader() throws Exception {
    // http://genome.ucsc.edu/goldenpath/help/bigBed.html
    try (final SeekableDataInput s = SeekableDataInput.of(Examples.get("example1.bb"))) {
      final BigFile.Header bigHeader = BigFile.Header.parse(s);
      assertTrue(bigHeader.version == 1);
      assertTrue(bigHeader.zoomLevels == 5);
      assertTrue(bigHeader.unzoomedDataOffset == 233);
      assertTrue(bigHeader.fieldCount == 3);
      assertTrue(bigHeader.definedFieldCount == 3);
      assertTrue(bigHeader.asOffset >= 0);
      assertTrue(bigHeader.totalSummaryOffset >= 0);
      assertTrue(bigHeader.uncompressBufSize == 0);
      assertTrue(bigHeader.bPlusTree.header.blockSize == 1);
      assertTrue(bigHeader.bPlusTree.header.keySize == 5);
      assertTrue(bigHeader.bPlusTree.header.valSize == 8);
      assertTrue(bigHeader.bPlusTree.header.itemCount == 1);
      assertTrue(bigHeader.bPlusTree.header.rootOffset == 216);
    }
  }

}