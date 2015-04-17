package org.jbb.big;

import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

public class BPlusTreeWriterTest extends TestCase {

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
      final ArrayList<BPlusLeaf> bPlusLeafs = loadBPlusLeafs();
      BPlusTree.Header.write(writer, bPlusLeafs, blockSize);
      writer.close();

      final SeekableDataInput reader = SeekableDataInput.of(path, byteOrder);
      final BPlusTree bplusTree = BPlusTree.read(reader, 0);
      final Optional<BPlusLeaf> leaf = bplusTree.find(reader, "chr01");
      assertTrue(leaf.isPresent());
      assertEquals(0, leaf.get().id);
      assertEquals(249250621, leaf.get().size);

      final ImmutableList.Builder<String> b = ImmutableList.builder();
      bplusTree.traverse(reader, bpl -> b.add(bpl.key));
      final ImmutableList<String> chromosomeNames = b.build();
      assertEquals(bPlusLeafs.size(), chromosomeNames.size());
      for (int i = 0; i < bPlusLeafs.size(); ++i) {
        assertEquals(bPlusLeafs.get(i).key, chromosomeNames.get(i));
      }

    }
    finally {
      Files.deleteIfExists(path);
    }

  }
}