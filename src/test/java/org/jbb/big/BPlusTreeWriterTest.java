package org.jbb.big;

import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import org.junit.Assert;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

public class BPlusTreeWriterTest extends TestCase {

  private static final int NUM_ATTEMPTS = 5;
  private static final int MAX_CHROMOSOME_COUNT = 1000;
  private static final Random random = new Random(5);

  private ArrayList<BPlusLeaf> loadBPlusLeafs() throws IOException, URISyntaxException {
    final ArrayList<BPlusLeaf> result = new ArrayList<>();
    final Path path = Examples.get("f1.chrom.sizes");
    final BufferedReader reader = new BufferedReader(new FileReader(path.toFile()));
    String buffer;
    int id = 0;
    while ((buffer = reader.readLine()) != null) {
      final String[] params = buffer.split("\t");
      result.add(new BPlusLeaf(params[0], id++, Integer.valueOf(params[1])));
    }
    reader.close();
    Collections.sort(result, (BPlusLeaf a, BPlusLeaf b) -> (a.key.compareTo(b.key)));
    return result;
  }

  public void testWrite() throws Exception {
    final Path path = Files.createTempFile("BPlusTree", ".bb");
    final ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;
    try {
      final int blockSize = 4;
      final SeekableDataOutput writer = SeekableDataOutput.of(path, byteOrder);
      final ArrayList<BPlusLeaf> leafs = loadBPlusLeafs();
      BPlusTree.Header.write(writer, leafs, blockSize);
      writer.close();


      final SeekableDataInput reader = SeekableDataInput.of(path, byteOrder);
      final BPlusTree bplusTree = BPlusTree.read(reader, 0);

      checkFind(bplusTree, reader, leafs);
      checkTraverse(bplusTree, reader, leafs);

    }
    finally {
      Files.deleteIfExists(path);
    }

  }

  private ArrayList<BPlusLeaf> generateBPlusLeafs() {
    final ArrayList<BPlusLeaf> result = new ArrayList<>();

    int count = random.nextInt(MAX_CHROMOSOME_COUNT);
    int id = 0;
    HashSet<Integer> names = new HashSet<Integer>();
    while (names.size() != count) {
      names.add(random.nextInt(MAX_CHROMOSOME_COUNT * 100));
    }
    Iterator it = names.iterator();
    for (int i = 0; i < count; ++i) {
      String chromosomeName = "chr" + it.next();
      int size = random.nextInt(100000);
      result.add(new BPlusLeaf(chromosomeName, id++, size));
    }
    Collections.sort(result, (BPlusLeaf a, BPlusLeaf b) -> (a.key.compareTo(b.key)));
    return result;
  }

  private void checkFind(BPlusTree bplusTree, SeekableDataInput reader, ArrayList<BPlusLeaf> leafs) throws IOException {

    for (BPlusLeaf leaf: leafs) {
      final Optional<BPlusLeaf> findLeaf = bplusTree.find(reader, leaf.key);
      Assert.assertTrue(findLeaf.isPresent());
      Assert.assertTrue(leaf.equals(findLeaf.get()));
    }
  }

  private void checkTraverse(BPlusTree bplusTree, SeekableDataInput reader, ArrayList<BPlusLeaf> leafs)
      throws IOException {
    final ImmutableList.Builder<String> b = ImmutableList.builder();
    bplusTree.traverse(reader, bpl -> b.add(bpl.key));
    final ImmutableList<String> chromosomeNames = b.build();
    Assert.assertEquals(leafs.size(), chromosomeNames.size());
    for (int i = 0; i < leafs.size(); ++i) {
      Assert.assertEquals(leafs.get(i).key, chromosomeNames.get(i));
    }
  }

  public void testCheckRandomChromosomes() throws IOException {
    for (int i = 0; i < NUM_ATTEMPTS; ++i) {

      final Path path = Files.createTempFile("BPlusTree", ".bb");
      final ByteOrder byteOrder = ((i & 1) == 0) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
      final SeekableDataOutput writer = SeekableDataOutput.of(path, byteOrder);

      ArrayList<BPlusLeaf> leafs = generateBPlusLeafs();
      int blockSize = random.nextInt(MAX_CHROMOSOME_COUNT);

      BPlusTree.Header.write(writer, leafs, blockSize);
      writer.close();

      final SeekableDataInput reader = SeekableDataInput.of(path, byteOrder);
      final BPlusTree bplusTree = BPlusTree.read(reader, 0);


      checkFind(bplusTree, reader, leafs);
      checkTraverse(bplusTree, reader, leafs);

    }
  }
}