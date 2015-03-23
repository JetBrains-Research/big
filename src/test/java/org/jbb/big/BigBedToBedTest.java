package org.jbb.big;

import junit.framework.TestCase;

import java.net.URISyntaxException;
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
      assertEquals(countLines(outputPath), maxItems);
//      showFileContent(outputPath);
    } finally {
      Files.deleteIfExists(outputPath);
    }
    // Restrict intervals
    chromStart = 9508110;
    chromEnd = 9906613;
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    System.out.println(countLines(outputPath));
    try {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
      // In example1.bb we have only one chromosome
      assertEquals(countLines(outputPath), 5);
      showFileContent(outputPath);
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

    Optional<BPlusLeaf> bptNodeLeaf
        = BPlusTree.find(s, bigHeader.bptHeader, "chr1");
    assertFalse(bptNodeLeaf.isPresent());

    bptNodeLeaf = BPlusTree.find(s, bigHeader.bptHeader, "chr21");
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
    s.order(bigHeader.bptHeader.byteOrder);
    BPlusTree.traverse(s, bigHeader.bptHeader, chromList::add);

    final RTreeIndexHeader rtiHeader
        = RTreeIndexHeader.read(s, bigHeader.unzoomedIndexOffset);

    // Loop through chromList in reverse
    final Iterator<BPlusLeaf> iter = chromList.descendingIterator();
    final BPlusLeaf node = iter.next();
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

  private Path getExample(final String name) throws URISyntaxException {
    final URL url = getClass().getClassLoader().getResource(name);
    if (url == null) {
      throw new IllegalStateException("resource not found");
    }
    return Paths.get(url.toURI()).toFile().toPath();
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

  private static int countLines(Path path) throws Exception {
    InputStream is = new BufferedInputStream(new FileInputStream(path.toFile()));
    try {
      byte[] c = new byte[1024];
      int count = 0;
      int readChars = 0;
      boolean empty = true;
      while ((readChars = is.read(c)) != -1) {
        empty = false;
        for (int i = 0; i < readChars; ++i) {
          if (c[i] == '\n') {
            ++count;
          }
        }
      }
      return (count == 0 && !empty) ? 1 : count;
    } finally {
      is.close();
    }
  }
}