package org.jbb.big;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

public class RTreeIndexTest extends TestCase {
  public void testParseHeader() throws IOException {
    try (final BigBedFile bbf = BigBedFile.parse(Examples.get("example1.bb"))) {
      final RTreeIndex rti = bbf.header.rTree;
      assertEquals(1024, rti.header.blockSize);
      assertEquals(192771, rti.header.fileSize);
      assertEquals(64, rti.header.itemsPerSlot);
      assertEquals(192819, rti.header.rootOffset);

      final BPlusTree bpt = bbf.header.bPlusTree;
      final List<BedData> items = getExampleItems(Examples.get("example1.bed"), name -> {
        try {
          return bpt.find(bbf.handle, name).get().id;
        } catch (IOException e) {
          return -1;  // Why are you doing this with me, Java?
        }
      });

      final int dummy = Integer.MIN_VALUE;
      assertEquals(items.size(), rti.header.itemCount);
      assertEquals(items.stream().mapToInt(item -> item.id).min().orElse(dummy),
                   rti.header.startChromIx);
      assertEquals(items.stream().mapToInt(item -> item.start).min().orElse(dummy),
                   rti.header.startBase);
      assertEquals(items.stream().mapToInt(item -> item.id).max().orElse(dummy),
                   rti.header.endChromIx);
      assertEquals(items.stream().mapToInt(item -> item.end).max().orElse(dummy),
                   rti.header.endBase);
    }
  }

  private List<BedData> getExampleItems(final Path path, final ToIntFunction<String> resolver)
      throws IOException {
    return Files.lines(path).map(line -> {
      final String[] chunks = line.split("\t", 3);
      return new BedData(resolver.applyAsInt(chunks[0]),
                         Integer.parseInt(chunks[1]),
                         Integer.parseInt(chunks[2]), "");
    }).collect(Collectors.toList());
  }
}