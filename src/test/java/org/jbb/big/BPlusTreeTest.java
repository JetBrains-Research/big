package org.jbb.big;

import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Optional;

public class BPlusTreeTest extends TestCase {

  public void testFind() throws Exception {
    final Path inputPath = Examples.get("example1.bb");
    final BigBedFile bf = BigBedFile.parse(inputPath);

    Optional<BPlusLeaf> bptNodeLeaf = bf.header.bPlusTree.find(bf.handle, "chr1");
    assertFalse(bptNodeLeaf.isPresent());

    bptNodeLeaf = bf.header.bPlusTree.find(bf.handle, "chr21");
    assertTrue(bptNodeLeaf.isPresent());
    assertEquals(0, bptNodeLeaf.get().id);
    assertEquals(48129895, bptNodeLeaf.get().size);
  }

  private void testFindAllBase(Path path,long chromTreeOffset, String[] chromosomes)
      throws IOException {
    SeekableDataInput reader = SeekableDataInput.of(path);
    BPlusTree.Header header = BPlusTree.Header.read(reader, chromTreeOffset);
    BPlusTree tree = new BPlusTree(header);

    final ImmutableList.Builder<String> b = ImmutableList.builder();
    tree.traverse(reader, bpl -> b.add(bpl.key));
    final ImmutableList<String> chromosomeNames = b.build();


    for (String key : chromosomes) {
      Optional<BPlusLeaf> bptNodeLeaf = tree.find(reader, key);
      assertTrue(bptNodeLeaf.isPresent());
    }
    Optional<BPlusLeaf> bptNodeLeaf = tree.find(reader, "chr_Vasia");
    assertFalse(bptNodeLeaf.isPresent());

  }

  public void testFindAllEqualSize() throws URISyntaxException, IOException {
    String[] chromosomes = {"chr01",
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