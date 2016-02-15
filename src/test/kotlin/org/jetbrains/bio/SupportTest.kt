package org.jetbrains.bio

import com.google.common.math.IntMath
import org.junit.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SupportTest {
    @Test fun logCeiling() {
        assertEquals(2, 4.logCeiling(2))
        assertEquals(3, 5.logCeiling(2))
        assertEquals(3, 6.logCeiling(2))
        assertEquals(3, 7.logCeiling(2))

        val r = Random()
        for (i in 0 until 100) {
            val a = r.nextInt(4095) + 1
            val b = r.nextInt(a) + 2
            val n = a.logCeiling(b)
            assertTrue(IntMath.pow(b, n) >= a, "ceil(log($a, base = $b)) /= $n")
        }
    }

    @Test fun groupingLazyEmpty() {
        val it = emptySequence<Int>().groupingBy { it }.iterator()
        assertFalse(it.hasNext())
    }

    @Test fun groupingLazySingleGroup() {
        val s = sequenceOf(2, 4, 6, 8)
        val it = s.groupingBy { it % 2 }.iterator()
        assertTrue(it.hasNext())
        val (key, g) = it.next()
        assertEquals(0, key)
        assertEquals(s.toList(), g.toList())
    }

    @Test fun groupingLazyTwoGroups() {
        val s = sequenceOf(2, 4, 7, 9)
        val it = s.groupingBy { it % 2 }.iterator()
        assertTrue(it.hasNext())
        val (key1, g1) = it.next()
        assertEquals(0, key1)
        assertEquals(listOf(2, 4), g1.toList())
        val (key2, g2) = it.next()
        assertEquals(1, key2)
        assertEquals(listOf(7, 9), g2.toList())
    }

    @Test fun groupingNonMonotonic() {
        val it = sequenceOf(2, 4, 7, 9, 6, 8).groupingBy { it % 2 }.iterator()
        it.next().second.toList()  // consume 2, 4
        it.next().second.toList()  // consume 7, 9
        val (key, g) = it.next()
        assertEquals(0, key)
        assertEquals(listOf(6, 8), g.toList())
    }

    @Test fun groupingReuse() {
        val it = sequenceOf(2).groupingBy { it % 2 }
        assertEquals(listOf(2), it.iterator().next().second.toList())
        assertEquals(listOf(2), it.iterator().next().second.toList())
    }
}
