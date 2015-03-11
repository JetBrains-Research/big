package org.jbb.big;

import junit.framework.TestCase;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BigBedToBedTest extends TestCase {
  public void testParseBigHeader() throws Exception {
    // http://genome.ucsc.edu/goldenpath/help/bigBed.html
    final URL url = getClass().getClassLoader().getResource("example1.bb");
    assert url != null : "resource not found";
    final BigHeader bigHeader = BigHeader.parse(Paths.get(url.getPath()));
    assertTrue(bigHeader.version == 1);
    assertTrue(bigHeader.zoomLevels == 5);
    assertTrue(bigHeader.chromTreeOffset == 184);
    assertTrue(bigHeader.unzoomedDataOffset == 233);
    assertTrue(bigHeader.unzoomedIndexOffset == 192771);
    assertTrue(bigHeader.fieldCount == 3);
    assertTrue(bigHeader.definedFieldCount == 3);
    assertTrue(bigHeader.asOffset >= 0);
    assertTrue(bigHeader.totalSummaryOffset >= 0);
    assertTrue(bigHeader.uncompressBufSize >= 0);
    assertTrue(bigHeader.bptHeader.blockSize == 1);
    assertTrue(bigHeader.bptHeader.keySize == 5);
    assertTrue(bigHeader.bptHeader.valSize == 8);
    assertTrue(bigHeader.bptHeader.itemCount == 1);
    assertTrue(bigHeader.bptHeader.rootOffset == 216);
  }

  public void testRTreeIndexHeader() throws Exception {
    final URL url = getClass().getClassLoader().getResource("example1.bb");
    assert url != null : "resource not found";
    final Path path = Paths.get(url.getPath());
    final SeekableStream s = SeekableStream.of(path);
    final long unzoomedIndexOffset = 192771;
    final RTreeIndexHeader rTreeIndexHeader = RTreeIndexHeader.read(s, unzoomedIndexOffset);
    assertEquals(rTreeIndexHeader.blockSize, 1024);
    assertEquals(rTreeIndexHeader.itemCount, 14810);
    assertEquals(rTreeIndexHeader.startChromIx, 0);
    assertEquals(rTreeIndexHeader.startBase, 9434178);
    assertEquals(rTreeIndexHeader.endChromIx, 0);
    assertEquals(rTreeIndexHeader.endBase, 48099781);
    assertEquals(rTreeIndexHeader.fileSize, 192771);
    assertEquals(rTreeIndexHeader.itemsPerSlot, 64);
    assertEquals(rTreeIndexHeader.rootOffset, 192819);
  }

  public void testRercursiveTraverseBPTree() throws Exception {
    final URL url = getClass().getClassLoader().getResource("example1.bb");
    assert url != null : "resource not found";
    BigBedToBed.parse(Paths.get(url.getPath()), "", 0, 0, 0);
  }
}