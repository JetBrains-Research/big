package org.jbb.big;

import junit.framework.TestCase;

public class RTreeIntervalTest extends TestCase {
  public void testOverlaps() throws Exception {
    final int ch1Id = 1;
    final int ch2Id = 2;
    final int ch3Id = 3;
    final int ch4Id = 4;
    // Simplest cases, when we have one chrom Id in intervals to compare
    assertEquals(overlaps(ch1Id, 100, ch1Id, 200, ch1Id, 300, ch1Id, 400), false);
    assertEquals(overlaps(ch1Id, 100, ch1Id, 200, ch1Id, 180, ch1Id, 400), true);
    assertEquals(overlaps(ch1Id, 100, ch1Id, 200, ch1Id, 20, ch1Id, 150), true);
    assertEquals(overlaps(ch1Id, 100, ch1Id, 200, ch1Id, 50, ch1Id, 250), true);
    assertEquals(overlaps(ch1Id, 100, ch1Id, 200, ch1Id, 150, ch1Id, 170), true);
    assertEquals(overlaps(ch1Id, 100, ch1Id, 200, ch1Id, 200, ch1Id, 300), false);
    // One interval belong to chrom1, another one to chrom2
    assertEquals(overlaps(ch1Id, 100, ch1Id, 200, ch2Id, 200, ch2Id, 300), false);
    // Fully different chrom ids in offsets
    assertEquals(overlaps(ch1Id, 100, ch2Id, 200, ch3Id, 150, ch4Id, 300), false);
    // Other mixes of chrom ids
    assertEquals(overlaps(ch1Id, 100, ch2Id, 200, ch2Id, 150, ch3Id, 300), true);
    assertEquals(overlaps(ch1Id, 100, ch2Id, 200, ch2Id, 300, ch3Id, 400), false);
    assertEquals(overlaps(ch1Id, 100, ch2Id, 200, ch2Id, 50, ch3Id, 300), true);
    assertEquals(overlaps(ch1Id, 100, ch3Id, 200, ch2Id, 50, ch3Id, 300), true);
    assertEquals(overlaps(ch1Id, 100, ch3Id, 200, ch2Id, 300, ch3Id, 400), true);
    assertEquals(overlaps(ch1Id, 100, ch3Id, 200, ch2Id, 50, ch3Id, 80), true);
  }

  /**
   * Construct two RTreeInverval's from source data and looking for overlaps
   * @param firstChromIdLeft first interval, left chrom id
   * @param firstOffsetLeft first interval, left offset
   * @param firstChromIdRight first interval, right chrom id
   * @param firstOffsetRight first interval, right offset
   * @param secondChromIdLeft second interval, left chrom id
   * @param secondOffsetLeft second interval, left offset
   * @param secondChromIdRight second interval, right chrom id
   * @param secondOffsetRight second interval, right offset
   * @return boolean
   */
  private static boolean overlaps(
      final int firstChromIdLeft,
      final int firstOffsetLeft,
      final int firstChromIdRight,
      final int firstOffsetRight,
      final int secondChromIdLeft,
      final int secondOffsetLeft,
      final int secondChromIdRight,
      final int secondOffsetRight) {
    final RTreeInterval interval1 = new RTreeInterval(
        new RTreeOffset(firstChromIdLeft, firstOffsetLeft),
        new RTreeOffset(firstChromIdRight, firstOffsetRight)
    );
    final RTreeInterval interval2 = new RTreeInterval(
        new RTreeOffset(secondChromIdLeft, secondOffsetLeft),
        new RTreeOffset(secondChromIdRight, secondOffsetRight)
    );
    return interval1.overlaps(interval2);
  }
}