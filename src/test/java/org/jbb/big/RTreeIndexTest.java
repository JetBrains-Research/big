package org.jbb.big;

import com.google.common.collect.Maps;

import junit.framework.TestCase;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class RTreeIndexTest extends TestCase {
  private final Random RANDOM = new Random();

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

  @NotNull
  private BigBedFile getExampleFile() throws IOException {
    return BigBedFile.parse(Examples.get("example1.bb"));
  }

  public void testFindSingleOverlappingBlock() throws IOException {
    final List<BedData> items = getExampleItems();
    try (final BigBedFile bbf = getExampleFile()) {
      for (int i = 0; i < 10; i++) {
        final BedData item = items.get(RANDOM.nextInt(items.size()));
        final RTreeInterval query = RTreeInterval.of(item.id, item.start, item.end);
        final List<BedData> overlaps = bbf.queryInternal(query, -1);
        assertEquals(1, overlaps.size());
        assertEquals(item, overlaps.get(0));
      }
    }
  }

  public void testFindMultipleOverlappingBlocks() throws IOException {
    final List<BedData> items = getExampleItems();
    final int[] offsets = items.stream().mapToInt(item -> item.start).toArray();
    try (final BigBedFile bbf = getExampleFile()) {
      for (int i = 0; i < 10; i++) {
        final int a = offsets[RANDOM.nextInt(offsets.length)];
        final int b = a + 1000; // offsets[RANDOM.nextInt(offsets.length)];
        final RTreeInterval query = RTreeInterval.of(
            0,  // example contains a single chromosome.
            Math.min(a, b),
            Math.max(a, b));
        final List<BedData> overlaps = bbf.queryInternal(query, -1);
        final List<BedData> expected = items.stream()
            .filter(item -> item.start >= query.left.offset && item.end <= query.right.offset)
            .collect(Collectors.toList());

        assertEquals(expected.size(), overlaps.size());
        assertEquals(expected, overlaps);
      }
    }
  }

  private List<BedData> getExampleItems() throws IOException {
    final Map<String, Integer> chromosomes = Maps.newHashMap();
    try (final BigBedFile bbf = getExampleFile()) {
      final BPlusTree bpt = bbf.header.bPlusTree;
      bpt.traverse(bbf.handle, item -> chromosomes.put(item.key, item.id));
    }

    return Files.lines(Examples.get("example1.bed")).map(line -> {
      final String[] chunks = line.split("\t", 3);
      return new BedData(chromosomes.get(chunks[0]),
                         Integer.parseInt(chunks[1]),
                         Integer.parseInt(chunks[2]), "");
    }).collect(Collectors.toList());
  }
}