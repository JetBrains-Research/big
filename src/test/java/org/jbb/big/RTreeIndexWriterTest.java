package org.jbb.big;

import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class RTreeIndexWriterTest extends TestCase {

  private void checkQuery(final RTreeIndex rti, final SeekableDataInput reader,
                          final RTreeInterval query, final List<RTreeIndexLeaf> expected)
      throws IOException {
    final List<RTreeIndexLeaf> actual = new ArrayList<>();
    rti.findOverlappingBlocks(reader, query, actual::add);

    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      // We don't compare the intervals, because 'expected' holds dummies.
      assertEquals(expected.get(i).getDataOffset(),
                   actual.get(i).getDataOffset());
      assertEquals(expected.get(i).getDataSize(),
                   actual.get(i).getDataSize());
    }
  }

  public void test0() throws IOException {
    final Path chromSizesPath = Examples.get("f2.chrom.sizes");
    final Path bedFilePath = Examples.get("bedExample01.txt");
    final Path pathBigBedFile = Files.createTempFile("BPlusTree", ".bb");

    final long rTreeHeaderOffset;
    try (final SeekableDataOutput output = SeekableDataOutput.of(pathBigBedFile)) {
      // задается для B+ tree /* Number of items to bundle in r-tree.  1024 is good. */
      final int blockSize = 4;
      /* Number of items in lowest level of tree.  64 is good. */
      final int itemsPerSlot = 3;
      // Берется из as данных: bits16 fieldCount = slCount(as->columnList);
      final short fieldCount = 3;
      rTreeHeaderOffset = RTreeIndex.Header
          .write(output, chromSizesPath, bedFilePath, blockSize, itemsPerSlot, fieldCount);
    }

    try (final SeekableDataInput input = SeekableDataInput.of(pathBigBedFile)) {
      final RTreeIndex rti = RTreeIndex.read(input, rTreeHeaderOffset);

      assertEquals(rti.header.blockSize, 4);
      assertEquals(rti.header.itemCount, 13);
      assertEquals(rti.header.startChromIx, 0);
      assertEquals(rti.header.startBase, 9434178);
      assertEquals(rti.header.endChromIx, 10);
      assertEquals(rti.header.endBase, 13058276);
      assertEquals(rti.header.fileSize, 299);
      assertEquals(rti.header.itemsPerSlot, 1);
      assertEquals(rti.header.rootOffset, 347);

      final RTreeInterval dummy = RTreeInterval.of(0, 0, 0);
      checkQuery(rti, input,
                 RTreeInterval.of(0, 9434178, 9434611),
                 ImmutableList.of(new RTreeIndexLeaf(dummy, 0, 39),
                                  new RTreeIndexLeaf(dummy, 39, 13)));

      checkQuery(rti, input, RTreeInterval.of(0, 9508110, 9516987),
                 ImmutableList.of());

      checkQuery(rti, input, RTreeInterval.of(1, 9508110, 9516987),
                 ImmutableList.of(new RTreeIndexLeaf(dummy, 52, 26)));

      checkQuery(rti, input, RTreeInterval.of(2, 9907597, 10148590),
                 ImmutableList.of(new RTreeIndexLeaf(dummy, 78, 39)));

      checkQuery(rti, input, RTreeInterval.of(2, 9908258, 10148590),
                 ImmutableList.of(new RTreeIndexLeaf(dummy, 78, 39)));

      checkQuery(rti, input, RTreeInterval.of(10, 13057621, 13058276),
                 ImmutableList.of(new RTreeIndexLeaf(dummy, 286, 13)));
    }
  }
}
