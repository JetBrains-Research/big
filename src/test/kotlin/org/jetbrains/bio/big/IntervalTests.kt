package org.jetbrains.bio.big

import org.junit.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class IntervalTest {
    @Test fun testOverlapsSameChromosome() {
        val interval = Interval(1, 100, 200)
        assertOverlaps(interval, interval)
        assertOverlaps(interval, Interval(1, 50, 150))
        assertOverlaps(interval, Interval(1, 150, 250))
        assertOverlaps(interval, Interval(1, 50, 250))
        assertOverlaps(interval, Interval(1, 150, 160))
        assertNotOverlaps(interval, Interval(1, 250, 300))
        // This is OK because right end is exclusive.
        assertNotOverlaps(interval, Interval(1, 200, 300))
    }

    @Test fun testOverlapsDifferentChromosomes() {
        assertNotOverlaps(Interval(1, 100, 200), Interval(2, 50, 150))
        assertNotOverlaps(Interval(1, 100, 200), Interval(2, 50, 3, 150))

        assertOverlaps(Interval(1, 100, 2, 200), Interval(2, 50, 3, 150))
        assertNotOverlaps(Interval(1, 100, 2, 200), Interval(2, 300, 3, 400))
        assertOverlaps(Interval(1, 100, 2, 200), Interval(2, 50, 3, 250))
        assertOverlaps(Interval(1, 100, 3, 200), Interval(2, 50, 3, 250))
        assertOverlaps(Interval(1, 100, 3, 200), Interval(2, 300, 3, 400))
        assertOverlaps(Interval(1, 100, 3, 200), Interval(2, 50, 3, 100))
    }

    private fun assertOverlaps(interval1: Interval, interval2: Interval) {
        assertTrue(interval1 intersects interval2, "$interval1 must overlap $interval2")
        assertTrue(interval2 intersects interval1, "$interval2 must overlap $interval1")
    }

    private fun assertNotOverlaps(interval1: Interval, interval2: Interval) {
        assertFalse(interval1 intersects interval2,
                    "$interval1 must not overlap $interval2")
        assertFalse(interval2 intersects interval1,
                    "$interval1 must not overlap $interval1")
    }

    @Test fun testSlice() {
        assertEquals(listOf(Interval(0, 0, 5), Interval(0, 5, 10)),
                     Interval(0, 0, 10).slice(2).toList())
        assertEquals(listOf(Interval(0, 0, 5), Interval(0, 5, 9)),
                     Interval(0, 0, 9).slice(2).toList())

        assertEquals(listOf(Interval(0, 0, 1),
                            Interval(0, 1, 3),
                            Interval(0, 3, 4),
                            Interval(0, 4, 5)),
                     Interval(0, 0, 5).slice(4).toList())

        for (i in 0 until 100) {
            val startOffset = RANDOM.nextInt(1000)
            val endOffset = startOffset + RANDOM.nextInt(999) + 1
            val interval = Interval(0, startOffset, endOffset)
            val n = Math.min(RANDOM.nextInt(99) + 1, interval.length())
            val slices = interval.slice(n).toList()

            assertEquals(n, slices.size())
            assertEquals(interval, slices.reduce(Interval::union))
            for (slice in slices) {
                assertTrue(slices.all { it == slice || !(it intersects slice) })
            }
        }
    }

    companion object {
        val RANDOM = Random()
    }
}

class OffsetTest {
    @Test fun testCompareToSameChromosome() {
        val offset = Offset(1, 100)
        assertTrue(offset > Offset(1, 50))
        assertTrue(Offset(1, 50) < offset)
        assertTrue(offset == offset)
    }

    @Test fun testCompareToDifferentChromosomes() {
        val offset = Offset(1, 100)
        assertTrue(offset < Offset(2, 100))
        assertTrue(Offset(2, 100) > offset)
        assertTrue(offset < Offset(2, 50))
        assertTrue(Offset(2, 50) > offset)
    }
}