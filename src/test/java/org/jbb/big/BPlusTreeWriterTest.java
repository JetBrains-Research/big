package org.jbb.big;

import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class BPlusTreeWriterTest extends TestCase {
  private static final int NUM_ATTEMPTS = 10;
  private static final int MAX_CHROMOSOME_COUNT = 100;
  private static final Random random = new Random(5);

  private List<BPlusLeaf> loadBPlusLeafs() throws IOException {
    final List<BPlusLeaf> result = new ArrayList<>();
    final Path path = Examples.get("f1.chrom.sizes");
    try (final BufferedReader reader = Files.newBufferedReader(path)) {
      String buffer;
      int id = 0;
      while ((buffer = reader.readLine()) != null) {
        final String[] params = buffer.split("\t");
        result.add(new BPlusLeaf(params[0], id++, Integer.parseInt(params[1])));
      }
    }

    Collections.sort(result, (BPlusLeaf a, BPlusLeaf b) -> a.key.compareTo(b.key));
    return result;
  }

  public void testWrite() throws Exception {
    final Path path = Files.createTempFile("BPlusTree", ".bb");
    final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
    try {
      final int blockSize = 3;
      final SeekableDataOutput writer = SeekableDataOutput.of(path, byteOrder);
      final List<BPlusLeaf> leafs = loadBPlusLeafs();
      BPlusTree.Header.write(writer, leafs, blockSize);
      writer.close();

      final SeekableDataInput reader = SeekableDataInput.of(path, byteOrder);
      final BPlusTree bplusTree = BPlusTree.read(reader, 0);
      checkFind(bplusTree, reader, leafs);
      checkTraverse(bplusTree, reader, leafs);
      reader.close();
    } finally {
      Files.deleteIfExists(path);
    }
  }

  private ArrayList<BPlusLeaf> generateBPlusLeafs() {
    final ArrayList<BPlusLeaf> result = new ArrayList<>();

    final int count = random.nextInt(MAX_CHROMOSOME_COUNT);
    int id = 0;
    final HashSet<Integer> names = new HashSet<>();
    while (names.size() != count) {
      names.add(random.nextInt(MAX_CHROMOSOME_COUNT * 100));
    }
    final Iterator it = names.iterator();
    for (int i = 0; i < count; ++i) {
      final String chromosomeName = "chr" + it.next();
      final int size = random.nextInt(100000);
      result.add(new BPlusLeaf(chromosomeName, id++, size));
    }
    Collections.sort(result, (BPlusLeaf a, BPlusLeaf b) -> (a.key.compareTo(b.key)));
    return result;
  }

  private void checkFind(final BPlusTree bplusTree, final SeekableDataInput reader,
                         final List<BPlusLeaf> leafs)
      throws IOException {
    for (final BPlusLeaf leaf: leafs) {
      final Optional<BPlusLeaf> findLeaf = bplusTree.find(reader, leaf.key);
      assertTrue(findLeaf.isPresent());
      assertTrue(leaf.equals(findLeaf.get()));
    }
  }

  private void checkTraverse(final BPlusTree bplusTree, final SeekableDataInput reader,
                             final List<BPlusLeaf> leafs)
      throws IOException {
    final ImmutableList.Builder<String> builder = ImmutableList.builder();
    bplusTree.traverse(reader, bpl -> builder.add(bpl.key));
    final ImmutableList<String> chromosomeNames = builder.build();
    assertEquals(leafs.size(), chromosomeNames.size());
    for (int i = 0; i < leafs.size(); ++i) {
      assertEquals(leafs.get(i).key, chromosomeNames.get(i));
    }
  }

  public void testCheckRandomChromosomes() throws IOException {
    for (int i = 0; i < NUM_ATTEMPTS; ++i) {
      final Path path = Files.createTempFile("BPlusTree", ".bb");
      final ByteOrder byteOrder = ((i & 1) == 0) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      final SeekableDataOutput writer = SeekableDataOutput.of(path, byteOrder);

      final ArrayList<BPlusLeaf> leafs = generateBPlusLeafs();
      final int blockSize = random.nextInt(MAX_CHROMOSOME_COUNT);

      BPlusTree.Header.write(writer, leafs, blockSize);
      writer.close();

      final SeekableDataInput reader = SeekableDataInput.of(path, byteOrder);
      final BPlusTree bplusTree = BPlusTree.read(reader, 0);

      checkFind(bplusTree, reader, leafs);
      checkTraverse(bplusTree, reader, leafs);
      reader.close();
    }
  }
}