package org.jbb.big

import org.junit.Assert
import org.junit.Test
import java.util.Random
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

public class InternalsTest {
    Test fun testLogCeiling() {
        assertEquals(2, 4.logCeiling(2))
        assertEquals(3, 5.logCeiling(2))
        assertEquals(3, 6.logCeiling(2))
        assertEquals(3, 7.logCeiling(2))

        for (i in 0 until 100) {
            val a = RANDOM.nextInt(4096) + 1
            val b = RANDOM.nextInt(a) + 2
            val n = a.logCeiling(b)
            assertTrue(b pow n >= a, "ceil(log($a, base = $b)) /= $n")
        }
    }

    Test fun testCompression() {
        for (i in 0 until 100) {
            val n = RANDOM.nextInt(4096) + 1
            val chunk = ByteArray(n)
            RANDOM.nextBytes(chunk)

            Assert.assertArrayEquals(chunk, chunk.compress().uncompress())
        }
    }

    companion object {
        private val RANDOM = Random()
    }
}

public class IntervalTest {
    Test fun testOverlapsSameChromosome() {
        val interval = Interval.of(1, 100, 200)
        assertOverlaps(interval, interval)
        assertOverlaps(interval, Interval.of(1, 50, 150))
        assertOverlaps(interval, Interval.of(1, 50, 250))
        assertOverlaps(interval, Interval.of(1, 150, 250))
        assertNotOverlaps(interval, Interval.of(1, 250, 300))
        // This is OK because right end is exclusive.
        assertNotOverlaps(interval, Interval.of(1, 200, 300))
    }

    Test fun testOverlapsDifferentChromosomes() {
        assertNotOverlaps(Interval.of(1, 100, 200), Interval.of(2, 50, 150))
        assertNotOverlaps(Interval.of(1, 100, 200), Interval.of(2, 50, 3, 150))

        assertOverlaps(Interval.of(1, 100, 2, 200), Interval.of(2, 50, 3, 150))
        assertNotOverlaps(Interval.of(1, 100, 2, 200), Interval.of(2, 300, 3, 400))
        assertOverlaps(Interval.of(1, 100, 2, 200), Interval.of(2, 50, 3, 250))
        assertOverlaps(Interval.of(1, 100, 3, 200), Interval.of(2, 50, 3, 250))
        assertOverlaps(Interval.of(1, 100, 3, 200), Interval.of(2, 300, 3, 400))
        assertOverlaps(Interval.of(1, 100, 3, 200), Interval.of(2, 50, 3, 100))
    }

    private fun assertOverlaps(interval1: Interval, interval2: Interval) {
        assertTrue(interval1.overlaps(interval2), "$interval1 must overlap $interval2")
        assertTrue(interval2.overlaps(interval1), "$interval2 must overlap $interval1")
    }

    private fun assertNotOverlaps(interval1: Interval, interval2: Interval) {
        assertFalse(interval1.overlaps(interval2),
                    "$interval1 must not overlap $interval2")
        assertFalse(interval2.overlaps(interval1),
                    "$interval1 must not overlap $interval1")
    }
}

public class OffsetTest {
    Test fun testCompareToSameChromosome() {
        val offset = Offset(1, 100)
        assertTrue(offset > Offset(1, 50))
        assertTrue(Offset(1, 50) < offset)
        assertTrue(offset == offset)
    }

    Test fun testCompareToDifferentChromosomes() {
        val offset = Offset(1, 100)
        assertTrue(offset < Offset(2, 100))
        assertTrue(Offset(2, 100) > offset)
        assertTrue(offset < Offset(2, 50))
        assertTrue(Offset(2, 50) > offset)
    }
}