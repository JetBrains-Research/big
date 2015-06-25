package org.jbb.big

import org.junit.Test
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

public class InternalsTest {
    Test fun testLogCeiling() {
        assertEquals(2, 4.logCeiling(2))
        assertEquals(3, 5.logCeiling(2))
        assertEquals(3, 6.logCeiling(2))
        assertEquals(3, 7.logCeiling(2))

        for (i in 0 until 100) {
            val a = RANDOM.nextInt(4096) + 1
            val b = RANDOM.nextInt(a) + 1
            val n = a.logCeiling(b)
            assertTrue(b pow n >= a, "ceil(log($a, base = $b)) /= $n")
        }
    }

    companion object {
        private val RANDOM = Random()
    }
}