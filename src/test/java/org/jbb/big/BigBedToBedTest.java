package org.jbb.big;

import junit.framework.TestCase;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
    assertTrue(bigHeader.bPlusTree.header.blockSize == 1);
    assertTrue(bigHeader.bPlusTree.header.keySize == 5);
    assertTrue(bigHeader.bPlusTree.header.valSize == 8);
    assertTrue(bigHeader.bPlusTree.header.itemCount == 1);
    assertTrue(bigHeader.bPlusTree.header.rootOffset == 216);
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

  public void testBigBedToBedFilterByChromosomeName() throws Exception {
    final Path inputPath = getExample("example1.bb");
    final Path outputPath = Files.createTempFile("out", ".bed");
    final int chromStart = 0;
    final int chromEnd = 0;
    final int maxItems = 0;
    // In example1.bb we have only chr21 chromosome
    String chromName = "chr22";
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try {
      assertTrue(Files.exists(outputPath));
      assertFalse(Files.size(outputPath) > 0);
    } finally {
      Files.deleteIfExists(outputPath);
    }
    // This chromosome exist in example bb-file
    chromName = "chr21";
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
    } finally {
      Files.deleteIfExists(outputPath);
    }
    // Get all chromosome from example file
    chromName = "";
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
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
    final String chromName = "chr21";
    int chromStart = 0;
    int chromEnd = 0;
    int maxItems = 10;
    // Check lines count in output file
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
      // In example1.bb we have only one chromosome
      assertEquals(maxItems, Files.lines(outputPath).count());
//      showFileContent(outputPath);
    } finally {
      Files.deleteIfExists(outputPath);
    }
    // Restrict intervals
    chromStart = 9508110;
    chromEnd = 9906613;
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    System.out.println(Files.lines(outputPath).count());
    try {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
      // In example1.bb we have only one chromosome
      assertEquals(Files.lines(outputPath).count(), 5);
      Files.copy(outputPath, System.out);
    } finally {
      Files.deleteIfExists(outputPath);
    }
  }


  public void testRTreeIndexHeader() throws Exception {
    final Path inputPath = getExample("example1.bb");
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

  public void testRFindChromName() throws Exception {
    final Path inputPath = getExample("example1.bb");
    final SeekableDataInput s = SeekableDataInput.of(inputPath);
    final BigHeader bigHeader = BigHeader.parse(s);

    Optional<BPlusLeaf> bptNodeLeaf = bigHeader.bPlusTree.find(s, "chr1");
    assertFalse(bptNodeLeaf.isPresent());

    bptNodeLeaf = bigHeader.bPlusTree.find(s, "chr21");
    assertTrue(bptNodeLeaf.isPresent());
    assertEquals(0, bptNodeLeaf.get().id);
    assertEquals(48129895, bptNodeLeaf.get().size);
  }

  public void testRFindOverlappingBlocks() throws Exception {
    final Path inputPath = getExample("example1.bb");
    final SeekableDataInput s = SeekableDataInput.of(inputPath);
    // Parse common headers
    final BigHeader bigHeader = BigHeader.parse(s);

    // Construct list of chromosomes from B+ tree
    final LinkedList<BPlusLeaf> chromList = new LinkedList<>();
    bigHeader.bPlusTree.traverse(s, chromList::add);

    final RTreeIndex rti = RTreeIndex.read(s, bigHeader.unzoomedIndexOffset);

    // Loop through chromList in reverse
    final Iterator<BPlusLeaf> iter = chromList.descendingIterator();
    final BPlusLeaf node = iter.next();
    final List<RTreeIndexLeaf> overlappingBlocks
        = rti.findOverlappingBlocks(s, RTreeInterval.of(node.id, 0, node.size));
    final ListIterator<RTreeIndexLeaf> iter2 = overlappingBlocks.listIterator();
    while (iter2.hasNext()) {
      RTreeIndexLeaf block = iter2.next();
//      System.out.println(block.dataOffset + "  " + block.dataSize);
    }
  }

  private Path getExample(final String name) throws URISyntaxException {
    final URL url = getClass().getClassLoader().getResource(name);
    if (url == null) {
      throw new IllegalStateException("resource not found");
    }
    return Paths.get(url.toURI()).toFile().toPath();
  }
}