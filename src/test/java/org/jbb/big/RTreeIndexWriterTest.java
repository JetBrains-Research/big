package org.jbb.big;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RTreeIndexWriterTest extends TestCase {

  private void checkQuery(final RTreeIndex rti, final SeekableDataInput reader, final RTreeOffset left, final RTreeOffset right, final List<RTreeIndexLeaf> target)
      throws IOException {
    final RTreeInterval query = new RTreeInterval(left, right);

    final ArrayList<RTreeIndexLeaf> result = new ArrayList<>();
    rti.findOverlappingBlocks(reader, query,
                              result::add);

    assertEquals(target.size(), result.size());
    for (int i = 0; i < result.size(); ++i) {
      assertTrue(result.get(i).equals(target.get(i)));
    }
  }
  public void test0() throws IOException {
    final Path chromSizesPath = Examples.get("f2.chrom.sizes");
    final Path bedFilePath = Examples.get("bedExample01.txt");
    final Path pathBigBedFile = Files.createTempFile("BPlusTree", ".bb");

    final SeekableDataOutput output = SeekableDataOutput.of(pathBigBedFile);
    final int blockSize = 4; // задается для B+ tree /* Number of items to bundle in r-tree.  1024 is good. */
    final int itemsPerSlot = 3; /* Number of items in lowest level of tree.  64 is good. */
    final short fieldCount = 3; // Берется из as данных: bits16 fieldCount = slCount(as->columnList);
    final long rTreeHeaderOffset = RTreeIndex.Header.write(output, chromSizesPath, bedFilePath, blockSize, itemsPerSlot, fieldCount);
    output.close();

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

      RTreeOffset left, right;
      List<RTreeIndexLeaf> target;
      final int chrom0 = 0;
      left = new RTreeOffset(chrom0, 9434178);
      right = new RTreeOffset(chrom0, 9434611);
      target = Arrays.asList(new RTreeIndexLeaf(0, 39), new RTreeIndexLeaf(39, 13));
      checkQuery(rti, input, left, right, target);

      left = new RTreeOffset(chrom0, 9508110);
      right = new RTreeOffset(chrom0, 9516987);
      target = new ArrayList<>();
      checkQuery(rti, input, left, right, target);

      final int chrom1 = 1;
      left = new RTreeOffset(chrom1, 9508110);
      right = new RTreeOffset(chrom1, 9516987);
      target = new ArrayList<>();
      target.add(new RTreeIndexLeaf(52, 26));

      checkQuery(rti, input, left, right, target);

      final int chrom2 = 2;
      left = new RTreeOffset(chrom2, 9907597);
      right = new RTreeOffset(chrom2, 10148590);
      target = new ArrayList<>();
      target.add(new RTreeIndexLeaf(78, 39));
      checkQuery(rti, input, left, right, target);

      left = new RTreeOffset(chrom2, 9908258);
      right = new RTreeOffset(chrom2, 10148590);
      target = new ArrayList<>();
      target.add(new RTreeIndexLeaf(78, 39));
      checkQuery(rti, input, left, right, target);


      final int chrom10 = 10;
      left = new RTreeOffset(chrom10, 13057621);
      right = new RTreeOffset(chrom10, 13058276);
      target = new ArrayList<>();
      target.add(new RTreeIndexLeaf(286, 13));
      checkQuery(rti, input, left, right, target);
    }
  }
}
