package org.jbb.big;

import junit.framework.TestCase;

import java.nio.file.Path;
import java.util.Optional;

public class BPlusTreeTest extends TestCase {
  public void testFind() throws Exception {
    final Path inputPath = Examples.get("example1.bb");
    final SeekableDataInput s = SeekableDataInput.of(inputPath);
    final BigHeader bigHeader = BigHeader.parse(s);

    Optional<BPlusLeaf> bptNodeLeaf = bigHeader.bPlusTree.find(s, "chr1");
    assertFalse(bptNodeLeaf.isPresent());

    bptNodeLeaf = bigHeader.bPlusTree.find(s, "chr21");
    assertTrue(bptNodeLeaf.isPresent());
    assertEquals(0, bptNodeLeaf.get().id);
    assertEquals(48129895, bptNodeLeaf.get().size);
  }
}