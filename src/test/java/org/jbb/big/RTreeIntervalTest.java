package org.jbb.big;

import junit.framework.TestCase;

public class RTreeIntervalTest extends TestCase {
  public void testOverlapsSameChromosome() {
    final RTreeInterval interval = RTreeInterval.of(1, 100, 200);
    assertOverlaps(interval, interval);
    assertOverlaps(interval, RTreeInterval.of(1, 50, 150));
    assertOverlaps(interval, RTreeInterval.of(1, 50, 250));
    assertOverlaps(interval, RTreeInterval.of(1, 150, 250));
    assertNotOverlaps(interval, RTreeInterval.of(1, 250, 300));
    // This is OK because right end is exclusive.
    assertNotOverlaps(interval, RTreeInterval.of(1, 200, 300));
  }

  public void testOverlapsDifferentChromosomes() {
    assertNotOverlaps(RTreeInterval.of(1, 100, 200), RTreeInterval.of(2, 50, 150));
    assertNotOverlaps(RTreeInterval.of(1, 100, 200), RTreeInterval.of(2, 50, 3, 150));

    assertOverlaps(RTreeInterval.of(1, 100, 2, 200), RTreeInterval.of(2, 50, 3, 150));
    assertNotOverlaps(RTreeInterval.of(1, 100, 2, 200), RTreeInterval.of(2, 300, 3, 400));
    assertOverlaps(RTreeInterval.of(1, 100, 2, 200), RTreeInterval.of(2, 50, 3, 250));
    assertOverlaps(RTreeInterval.of(1, 100, 3, 200), RTreeInterval.of(2, 50, 3, 250));
    assertOverlaps(RTreeInterval.of(1, 100, 3, 200), RTreeInterval.of(2, 300, 3, 400));
    assertOverlaps(RTreeInterval.of(1, 100, 3, 200), RTreeInterval.of(2, 50, 3, 100));
  }

  private void assertOverlaps(final RTreeInterval interval1, final RTreeInterval interval2) {
    assertTrue("%s must overlap %s", interval1.overlaps(interval2));
    assertTrue("%s must overlap %s", interval2.overlaps(interval1));
  }

  private void assertNotOverlaps(final RTreeInterval interval1, final RTreeInterval interval2) {
    assertFalse("%s must not overlap %s", interval1.overlaps(interval2));
    assertFalse("%s must not overlap %s", interval2.overlaps(interval1));
  }
}