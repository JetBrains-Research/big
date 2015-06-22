package org.jbb.big;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class BPlusTreeTest extends TestCase {

  public void testFind() throws Exception {
    final Path inputPath = Examples.get("example1.bb");
    try (final BigBedFile bf = BigBedFile.parse(inputPath)) {
      Optional<BPlusLeaf> bptNodeLeaf = bf.header.bPlusTree.find(bf.handle, "chr1");
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

    // blockSize = 3
    testFindAllBase(Examples.get("example2.bb"), chromosomes);

    // blockSize = 4
    testFindAllBase(Examples.get("example3.bb"), chromosomes);
  }

  public void testFindAllDifferentSize() throws IOException {
    final String[] chromosomes = {
        "chr1", "chr10", "chr11", "chr2", "chr3", "chr4", "chr5",
        "chr6", "chr7", "chr8", "chr9"
    };

    // blockSize = 4;
    testFindAllBase(Examples.get("example4.bb"), chromosomes);
  }

  private void testFindAllBase(final Path path, final String[] chromosomes)
      throws IOException {
    final long offset = 628;  // magic!
    try (final SeekableDataInput s = SeekableDataInput.of(path)) {
      final BPlusTree tree = BPlusTree.read(s, offset);
      for (final String key : chromosomes) {
        assertTrue(tree.find(s, key).isPresent());
      }

      assertFalse(tree.find(s, "chrV").isPresent());
    }
  }
}