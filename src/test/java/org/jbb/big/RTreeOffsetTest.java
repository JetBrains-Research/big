package org.jbb.big;

import junit.framework.TestCase;

public class RTreeOffsetTest extends TestCase {
  public void testCompareToSameChromosome() {
    final RTreeOffset offset = new RTreeOffset(1, 100);
    assertTrue(offset.compareTo(new RTreeOffset(1, 50)) > 0);
    assertTrue(new RTreeOffset(1, 50).compareTo(offset) < 0);
    assertTrue(offset.compareTo(offset) == 0);
  }

  public void testCompareToDifferentChromosomes() {
    final RTreeOffset offset = new RTreeOffset(1, 100);
    assertTrue(offset.compareTo(new RTreeOffset(2, 100)) < 0);
    assertTrue(new RTreeOffset(2, 100).compareTo(offset) > 0);
    assertTrue(offset.compareTo(new RTreeOffset(2, 50)) < 0);
    assertTrue(new RTreeOffset(2, 50).compareTo(offset) > 0);
  }
}