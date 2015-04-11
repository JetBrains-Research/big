package org.jbb.big;

import junit.framework.TestCase;

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
}