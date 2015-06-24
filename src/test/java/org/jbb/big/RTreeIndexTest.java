package org.jbb.big;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

public class RTreeIndexTest extends TestCase {
  public void testParseHeader() throws IOException {
    try (final BigBedFile bbf = getExampleFile()) {
      final RTreeIndex rti = bbf.header.rTree;
      assertEquals(1024, rti.header.blockSize);
      assertEquals(192771, rti.header.fileSize);
      assertEquals(64, rti.header.itemsPerSlot);
      assertEquals(192819, rti.header.rootOffset);

      final List<BedData> items = getExampleItems();
      final int dummy = Integer.MIN_VALUE;
      assertEquals(items.size(), rti.header.itemCount);
      assertEquals(0, rti.header.startChromIx);
      assertEquals(0, rti.header.endChromIx);
      assertEquals(items.stream().mapToInt(item -> item.start).min().orElse(dummy),
                   rti.header.startBase);
      assertEquals(items.stream().mapToInt(item -> item.end).max().orElse(dummy),
                   rti.header.endBase);
    }
  }

  private BigBedFile getExampleFile() throws IOException {
    return BigBedFile.parse(Examples.get("example1.bb"));
  }

  private List<BedData> getExampleItems() throws IOException {
    return Files.lines(Examples.get("example1.bed")).map(line -> {
      final String[] chunks = line.split("\t", 3);
      return new BedData(-1, // doesn't matter.
                         Integer.parseInt(chunks[1]),
                         Integer.parseInt(chunks[2]), "");
    }).collect(Collectors.toList());
  }
}