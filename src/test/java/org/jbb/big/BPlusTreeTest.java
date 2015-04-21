package org.jbb.big;

import junit.framework.TestCase;

import java.io.IOException;
import java.net.URISyntaxException;
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

  private void testFindAllBase(final Path path, final long chromTreeOffset,
                               final String[] chromosomes)
      throws IOException {
    try (final SeekableDataInput reader = SeekableDataInput.of(path)) {
      final BPlusTree.Header header = BPlusTree.Header.read(reader, chromTreeOffset);
      final BPlusTree tree = new BPlusTree(header);
      for (final String key : chromosomes) {
        assertTrue(tree.find(reader, key).isPresent());
      }

      assertFalse(tree.find(reader, "chr_Vasia").isPresent());
    }
  }

  public void testFindAllEqualSize() throws URISyntaxException, IOException {
    final String[] chromosomes = {"chr01",
                            "chr02",
                            "chr03",
                            "chr04",
                            "chr05",
                            "chr06",
                            "chr07",
                            "chr08",
                            "chr09",
                            "chr10",
                            "chr11",
    };
    // blockSize = 3
    final Path path2 = Examples.get("example2.bb");
    long chromTreeOffset = 628;
    testFindAllBase(path2, chromTreeOffset, chromosomes);

    // blockSize = 4
    final Path path3 = Examples.get("example3.bb");
    testFindAllBase(path3, chromTreeOffset, chromosomes);
  }

  public void testFindAllDifferentSize() throws IOException, URISyntaxException {
    String[] chromosomes = {"chr1",
                            "chr10",
                            "chr11",
                            "chr2",
                            "chr3",
                            "chr4",
                            "chr5",
                            "chr6",
                            "chr7",
                            "chr8",
                            "chr9"
    };
    // blockSize = 4;
    final Path path3 = Examples.get("example4.bb");
    long chromTreeOffset = 628;
    testFindAllBase(path3, chromTreeOffset, chromosomes);
  }

}