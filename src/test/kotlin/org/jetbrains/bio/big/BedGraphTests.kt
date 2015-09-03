package org.jetbrains.bio.big

import org.junit.Before
import org.junit.Test
import java.util.Random
import kotlin.properties.Delegates
import kotlin.test.assertEquals

public class BedGraphSectionTest {
    private var section: BedGraphSection by Delegates.notNull()

    @Before fun setUp() {
        section = BedGraphSection("chr1")
        section[100500, 100600] = 42.0f
        section[500100, 500300] = 24.0f
        section[500200, 500300] = 0f
        section[500200, 500400] = 0f
    }

    @Test fun testSet() {
        section[100500, 100600] = 42.0f
        assertEquals(42f + 42f, section[100500, 100600])
    }

    @Test fun testQueryNoBounds() {
        val correct = arrayOf(WigInterval(100500, 100600, 42f),
                              WigInterval(500100, 500300, 24f),
                              WigInterval(500200, 500300, 0f),
                              WigInterval(500200, 500400, 0f))

        assertEquals(4, section.size())
        assertEquals(correct.asList(), section.query().toList())
    }

    @Test fun testQuery() {
        val correct = arrayOf(WigInterval(100500, 100600, 42f),
                              WigInterval(500100, 500300, 24f),
                              WigInterval(500200, 500300, 0f))

        assertEquals(correct.asList(), section.query(100500, 500300).toList())

        // Make sure we don't include the hanging interval.
        section[500100, 500301] = Float.NEGATIVE_INFINITY
        assertEquals(correct.asList(), section.query(100500, 500300).toList())
    }

    @Test fun testSplice() {
        section = BedGraphSection("chr1")
        for (i in 0 until Short.MAX_VALUE * 2 - 100) {
            section[i, i + 1] = i.toFloat()
        }

        val subsections = section.splice().toList()
        assertEquals(2, subsections.size())
        assertEquals(0, subsections[0].start)
        assertEquals(Short.MAX_VALUE.toInt(), subsections[1].start)
        assertEquals(Short.MAX_VALUE.toInt(), subsections[0].size())
        assertEquals(Short.MAX_VALUE.toInt() - 100, subsections[1].size())
    }

    @Test fun testSpliceRandom() {
        for (i in 0 until 100) {
            section = BedGraphSection("chr1")
            val max = RANDOM.nextInt(99) + 1
            for (j in 0 until max * RANDOM.nextInt(99) + 1) {
                section[j, j + 1] = RANDOM.nextFloat()
            }

            val subsections = section.splice().toList()
            assertEquals(section.size(), subsections.map { it.size() }.sum())
            assertEquals(section.query().toList(),
                         subsections.map { it.query().toList() }.reduce { a, b -> a + b })
        }
    }

    companion object {
        private val RANDOM = Random()
    }
}
