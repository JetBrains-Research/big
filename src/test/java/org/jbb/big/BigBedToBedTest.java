package org.jbb.big;

import junit.framework.TestCase;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Optional;

public class BigBedToBedTest extends TestCase {
  public void testParseBigHeader() throws Exception {
    // http://genome.ucsc.edu/goldenpath/help/bigBed.html
    final Path inputPath = getExample("example1.bb");
    final SeekableDataInput s = SeekableDataInput.of(inputPath);
    final BigHeader bigHeader = BigHeader.parse(s);
    assertTrue(bigHeader.version == 1);
    assertTrue(bigHeader.zoomLevels == 5);
    assertTrue(bigHeader.chromTreeOffset == 184);
    assertTrue(bigHeader.unzoomedDataOffset == 233);
    assertTrue(bigHeader.unzoomedIndexOffset == 192771);
    assertTrue(bigHeader.fieldCount == 3);
    assertTrue(bigHeader.definedFieldCount == 3);
    assertTrue(bigHeader.asOffset >= 0);
    assertTrue(bigHeader.totalSummaryOffset >= 0);
    assertTrue(bigHeader.uncompressBufSize == 0);
    assertTrue(bigHeader.bptHeader.blockSize == 1);
    assertTrue(bigHeader.bptHeader.keySize == 5);
    assertTrue(bigHeader.bptHeader.valSize == 8);
    assertTrue(bigHeader.bptHeader.itemCount == 1);
    assertTrue(bigHeader.bptHeader.rootOffset == 216);
  }

  public void testBigBedToBed() throws Exception {
    final Path inputPath = getExample("example1.bb");
    final Path outputPath = Files.createTempFile("out", ".bed");
    BigBedToBed.main(inputPath, outputPath, "", 0, 0, 0);
    try {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
    } finally {
      Files.deleteIfExists(outputPath);
    }
  }

  public void testBigBedToBedRestrictOutput() throws Exception {
    final Path inputPath = getExample("example1.bb");
    final Path outputPath = Files.createTempFile("out", ".bed");
    // Params restriction
    final int chromStart = 0;
    final int chromEnd = 33996242;
    final int maxItems = 5;
    BigBedToBed.main(inputPath, outputPath, "", chromStart, chromEnd, maxItems);
    try {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
//      showFileContent(outputPath);
    } finally {
      Files.deleteIfExists(outputPath);
    }
  }

  public void testRTreeIndexHeader() throws Exception {
    final Path inputPath = getExample("example1.bb");
    final SeekableDataInput s = SeekableDataInput.of(inputPath);
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

  public void testRFindChromName() throws Exception {
    final Path inputPath = getExample("example1.bb");
    final SeekableDataInput s = SeekableDataInput.of(inputPath);
    final BigHeader bigHeader = BigHeader.parse(s);
    String chromName = "chr1";
    Optional<BptNodeLeaf> bptNodeLeaf
        = Bpt.chromFind(s.filePath(), bigHeader.bptHeader, chromName);
    assertFalse(bptNodeLeaf.isPresent());
    chromName = "chr21";
    bptNodeLeaf = Bpt.chromFind(s.filePath(), bigHeader.bptHeader, chromName);
    assertTrue(bptNodeLeaf.isPresent());
    assertEquals(bptNodeLeaf.get().id, 0);
    assertEquals(bptNodeLeaf.get().size, 48129895);
  }

  public void testRFindOverlappingBlocks() throws Exception {
    final Path inputPath = getExample("example1.bb");
    final SeekableDataInput s = SeekableDataInput.of(inputPath);
    // Parse common headers
    final BigHeader bigHeader = BigHeader.parse(s);

    // Construct list of chromosomes from B+ tree
    final LinkedList<BptNodeLeaf> chromList = new LinkedList<>();
    s.order(bigHeader.bptHeader.byteOrder);
    Bpt.rTraverse(s, bigHeader.bptHeader, bigHeader.bptHeader.rootOffset, chromList);

    final RTreeIndexHeader rtiHeader
        = RTreeIndexHeader.read(s, bigHeader.unzoomedIndexOffset);

    // Loop through chromList in reverse
    final Iterator<BptNodeLeaf> iter = chromList.descendingIterator();
    final BptNodeLeaf node = iter.next();
    final LinkedList<RTreeIndexLeaf> overlappingBlockList = new LinkedList<>();
    s.order(rtiHeader.byteOrder);
    RTreeIndex.rFindOverlappingBlocks(overlappingBlockList, s, 0,
                                      rtiHeader.rootOffset,
                                      RTreeRange.of(node.id, 0, node.size));
    Collections.reverse(overlappingBlockList);
    final ListIterator<RTreeIndexLeaf> iter2 = overlappingBlockList.listIterator();
    while (iter2.hasNext()) {
      RTreeIndexLeaf block = iter2.next();
//      System.out.println(block.dataOffset + "  " + block.dataSize);
    }
  }

  private Path getExample(final String name) {
    final URL url = getClass().getClassLoader().getResource(name);
    if (url == null) {
      throw new IllegalStateException("resource not found");
    }

    return Paths.get(url.getPath());
  }

  /**
   * Print file content to system output
   * @param path
   * @throws Exception
   */
  private static void showFileContent(final Path path) throws Exception {
    InputStream input = new BufferedInputStream(new FileInputStream(path.toFile()));
    byte[] buffer = new byte[8192];

    try {
      for (int length = 0; (length = input.read(buffer)) != -1;) {
        System.out.write(buffer, 0, length);
      }
    } finally {
      input.close();
    }
  }
}