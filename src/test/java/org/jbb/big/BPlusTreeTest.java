package org.jbb.big;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.jbb.big.BPlusTree.read;

public class BPlusTreeTest extends TestCase {
  private static final Random RANDOM = new Random();

  public void testFind() throws IOException {
    final Path path = Examples.get("example1.bb");
    try (final BigBedFile bf = BigBedFile.parse(path)) {
      Optional<BPlusItem> bptNodeLeaf = bf.header.bPlusTree.find(bf.handle, "chr1");
      assertFalse(bptNodeLeaf.isPresent());

      bptNodeLeaf = bf.header.bPlusTree.find(bf.handle, "chr21");
      assertTrue(bptNodeLeaf.isPresent());
      assertEquals(0, bptNodeLeaf.get().id);
      assertEquals(48129895, bptNodeLeaf.get().size);
    }
  }

  public void testFindAllEqualSize() throws IOException {
    final String[] chromosomes = {
        "chr01", "chr02", "chr03", "chr04", "chr05", "chr06", "chr07",
        "chr08", "chr09", "chr10", "chr11",
    };

    testFindAllExample("example2.bb", chromosomes);  // blockSize = 3.
    testFindAllExample("example3.bb", chromosomes);  // blockSize = 4.
  }

  public void testFindAllDifferentSize() throws IOException {
    final String[] chromosomes = {
        "chr1", "chr10", "chr11", "chr2", "chr3", "chr4", "chr5",
        "chr6", "chr7", "chr8", "chr9"
    };

    testFindAllExample("example4.bb", chromosomes);  // blockSize = 4.
  }

  private void testFindAllExample(final String example, final String[] chromosomes)
      throws IOException {
    final long offset = 628;  // magic!
    final Path path = Examples.get(example);
    try (final SeekableDataInput input = SeekableDataInput.of(path)) {
      final BPlusTree bpt = read(input, offset);
      for (final String key : chromosomes) {
        assertTrue(bpt.find(input, key).isPresent());
      }

      assertFalse(bpt.find(input, "chrV").isPresent());
    }
  }

  public void testCountLevels() {
    assertEquals(2, BPlusTree.countLevels(10, 100));
    assertEquals(2, BPlusTree.countLevels(10, 90));
    assertEquals(2, BPlusTree.countLevels(10, 11));
    assertEquals(1, BPlusTree.countLevels(10, 10));
  }

  public void testWriteReadSmall() throws IOException {
    testWriteRead(2, getSequentialItems(16));
    testWriteRead(2, getSequentialItems(7));  // not a power of 2.
  }

  public void testWriteReadLarge() throws IOException {
    testWriteRead(8, getSequentialItems(IntMath.pow(8, 3)));
  }

  private List<BPlusItem> getSequentialItems(final int itemCount) {
    return IntStream.rangeClosed(1, itemCount)
        .mapToObj(i -> new BPlusItem("chr" + i, i - 1, i * 100))
        .collect(Collectors.toList());
  }

  public void testWriteReadRandom() throws IOException {
    for (int i = 0; i < 10; i++) {
      final int blockSize = RANDOM.nextInt(64) + 1;
      testWriteRead(blockSize, getRandomItems(RANDOM.nextInt(1024) + 1));
    }
  }

  private List<BPlusItem> getRandomItems(final int itemCount) {
    final int[] names = RANDOM.ints(itemCount).distinct().toArray();
    return IntStream.range(0, names.length).mapToObj(i -> {
      final int size = Math.abs(RANDOM.nextInt()) + 1;
      return new BPlusItem("chr" + names[i], i, size);
    }).collect(Collectors.toList());
  }

  public void testWriteReadRealChromosomes() throws IOException {
    testWriteRead(3, getExampleItems("f1.chrom.sizes"));
    testWriteRead(4, getExampleItems("f2.chrom.sizes"));
  }

  private List<BPlusItem> getExampleItems(final String example) throws IOException {
    final Path path = Examples.get(example);
    final String[] lines = Files.lines(path).toArray(String[]::new);
    return IntStream.range(0, lines.length).mapToObj(i -> {
      final String[] chunks = lines[i].split("\t", 2);
      return new BPlusItem(chunks[0], i, Integer.parseInt(chunks[1]));
    }).collect(Collectors.toList());
  }

  private void testWriteRead(final int blockSize, final List<BPlusItem> items)
      throws IOException {
    final Path path = Files.createTempFile("bpt", ".bb");
    try {
      try (final SeekableDataOutput output = SeekableDataOutput.of(path)) {
        BPlusTree.write(output, blockSize, items);
      }

      try (final SeekableDataInput input = SeekableDataInput.of(path)) {
        final BPlusTree bpt = BPlusTree.read(input, 0);
        for (final BPlusItem item : items) {
          final String key = item.key;
          final Optional<BPlusItem> res = bpt.find(input, key);
          assertTrue(key, res.isPresent());
          assertEquals(key, item, res.get());
        }

        final Set<BPlusItem> actual = Sets.newHashSet();
        bpt.traverse(input, actual::add);
        assertEquals(ImmutableSet.copyOf(items), actual);
      }
    } finally {
      Files.delete(path);
    }
  }
}