package org.jbb.big

import junit.framework.TestCase
import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

public class RTreeIntervalTest {
    Test fun testOverlapsSameChromosome() {
        val interval = RTreeInterval.of(1, 100, 200)
        assertOverlaps(interval, interval)
        assertOverlaps(interval, RTreeInterval.of(1, 50, 150))
        assertOverlaps(interval, RTreeInterval.of(1, 50, 250))
        assertOverlaps(interval, RTreeInterval.of(1, 150, 250))
        assertNotOverlaps(interval, RTreeInterval.of(1, 250, 300))
        // This is OK because right end is exclusive.
        assertNotOverlaps(interval, RTreeInterval.of(1, 200, 300))
    }

    Test fun testOverlapsDifferentChromosomes() {
        assertNotOverlaps(RTreeInterval.of(1, 100, 200), RTreeInterval.of(2, 50, 150))
        assertNotOverlaps(RTreeInterval.of(1, 100, 200), RTreeInterval.of(2, 50, 3, 150))

        assertOverlaps(RTreeInterval.of(1, 100, 2, 200), RTreeInterval.of(2, 50, 3, 150))
        assertNotOverlaps(RTreeInterval.of(1, 100, 2, 200), RTreeInterval.of(2, 300, 3, 400))
        assertOverlaps(RTreeInterval.of(1, 100, 2, 200), RTreeInterval.of(2, 50, 3, 250))
        assertOverlaps(RTreeInterval.of(1, 100, 3, 200), RTreeInterval.of(2, 50, 3, 250))
        assertOverlaps(RTreeInterval.of(1, 100, 3, 200), RTreeInterval.of(2, 300, 3, 400))
        assertOverlaps(RTreeInterval.of(1, 100, 3, 200), RTreeInterval.of(2, 50, 3, 100))
    }

    private fun assertOverlaps(interval1: RTreeInterval, interval2: RTreeInterval) {
        assertTrue(interval1.overlaps(interval2), "$interval1 must overlap $interval2")
        assertTrue(interval2.overlaps(interval1), "$interval2 must overlap $interval1")
    }

    private fun assertNotOverlaps(interval1: RTreeInterval, interval2: RTreeInterval) {
        assertFalse(interval1.overlaps(interval2),
                    "$interval1 must not overlap $interval2")
        assertFalse(interval2.overlaps(interval1),
                    "$interval1 must not overlap $interval1")
    }
}

public class RTreeOffsetTest {
    Test fun testCompareToSameChromosome() {
        val offset = RTreeOffset(1, 100)
        assertTrue(offset > RTreeOffset(1, 50))
        assertTrue(RTreeOffset(1, 50) < offset)
        assertTrue(offset == offset)
    }

    Test fun testCompareToDifferentChromosomes() {
        val offset = RTreeOffset(1, 100)
        assertTrue(offset < RTreeOffset(2, 100))
        assertTrue(RTreeOffset(2, 100) > offset)
        assertTrue(offset < RTreeOffset(2, 50))
        assertTrue(RTreeOffset(2, 50) > offset)
    }
}