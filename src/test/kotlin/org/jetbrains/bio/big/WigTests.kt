package org.jetbrains.bio.big

import org.jetbrains.bio.ScoredInterval
import org.junit.Before
import org.junit.Test
import java.io.StringReader
import java.io.StringWriter
import java.util.*
import kotlin.properties.Delegates
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class VariableStepSectionTest {
    private var section: VariableStepSection by Delegates.notNull()

    @Before fun setUp() {
        section = VariableStepSection("chr1", 20)
        section[100500] = 42.0f
        section[500100] = 24.0f
    }

    @Test fun testSet() {
        section[100500] = 42.0f
        assertEquals(42f + 42f , section[100500])
    }

    @Test fun testQueryEmpty() {
        val emptySection = VariableStepSection("chr1", 20)
        assertEquals(emptyList<ScoredInterval>(), emptySection.query().toList())
        assertEquals(emptyList<ScoredInterval>(),
                     emptySection.query(100500, 500100).toList())
    }

    @Test fun testQueryNoBounds() {
        val correct = arrayOf(ScoredInterval(100500, 100520, 42f),
                              ScoredInterval(500100, 500120, 24f))

        assertEquals(correct.asList(), section.query().toList())
    }

    @Test fun testQuery() {
        // Add a few ranges from both sides.
        section[100100] = 4242.0f
        section[500500] = 2424.0f

        val correct = arrayOf(ScoredInterval(100500, 100520, 42f),
                              ScoredInterval(500100, 500120, 24f))

        // Note: why '500200'? because the 'end' value corresponds
        // to the right border of the rightmost 'Range', i. e.:
        //                 end
        //                  V
        // |----|----|----|----|----|
        //           +----+
        //       rightmost range
        assertEquals(correct.asList(), section.query(100500, 500200).toList())
    }

    @Test fun testSplice() {
        section = VariableStepSection("chr1", 20)
        for (i in 0 until Short.MAX_VALUE * 2 - 100) {
            section[i] = i.toFloat()
        }

        val subsections = section.splice().toList()
        assertEquals(2, subsections.size)
        assertEquals(0, subsections[0].start)
        assertEquals(Short.MAX_VALUE.toInt(), subsections[1].start)
        assertEquals(Short.MAX_VALUE.toInt(), subsections[0].size())
        assertEquals(Short.MAX_VALUE.toInt() - 100, subsections[1].size())
    }

    @Test fun testSpliceRandom() {
        for (i in 0 until 100) {
            section = VariableStepSection("chr1", RANDOM.nextInt(99) + 1)
            val max = RANDOM.nextInt(99) + 1
            for (j in 0 until max * RANDOM.nextInt(99) + 1) {
                section[j] = RANDOM.nextFloat()
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

class FixedStepSectionTest {
    private var section: FixedStepSection by Delegates.notNull()

    @Before fun setUp() {
        section = FixedStepSection("chr1", 400601, 100, 5)
        section.add(11f)
        section.add(22f)
        section.add(33f)
    }

    @Test fun testGet() {
        assertEquals(11f, section[400601])
        assertEquals(22f, section[400701])
        assertEquals(33f, section[400801])
    }

    @Test(expected = IndexOutOfBoundsException::class) fun testOutOfBounds() {
        section[400901]
    }

    @Test fun testQueryEmpty() {
        val emptySection = FixedStepSection("chr1", 400601, 100, 5)
        assertEquals(emptyList<ScoredInterval>(), emptySection.query().toList())
        assertEquals(emptyList<ScoredInterval>(),
                     emptySection.query(400601, 400801).toList())
    }

    @Test fun testQueryNoBounds() {
        val it = section.query().iterator()
        var interval: ScoredInterval
        interval = it.next()
        assertEquals(400601, interval.start)
        assertEquals(400606, interval.end)
        assertEquals(11f, interval.score)

        interval = it.next()
        assertEquals(400701, interval.start)
        assertEquals(400706, interval.end)
        assertEquals(22f, interval.score)

        interval = it.next()
        assertEquals(400801, interval.start)
        assertEquals(400806, interval.end)
        assertEquals(33f, interval.score)
        assertFalse(it.hasNext())
    }

    @Test fun testQuery() {
        // Note: the value '400600' will be rounded to the nearest
        // interval, in this case to '400601'.
        val it = section.query(400600, 400607).iterator()
        assertNotNull(it.next())
        assertFalse(it.hasNext())
    }

    @Test fun testQueryWithOverlaps() {
        val section = FixedStepSection("chr1", 400601, 10, 50)
        section.add(11f)
        section.add(22f)
        section.add(33f)

        val it = section.query().iterator()
        var interval: ScoredInterval
        interval = it.next()
        assertEquals(400601, interval.start)
        assertEquals(400651, interval.end)
        assertEquals(11f, interval.score)

        interval = it.next()
        assertEquals(400611, interval.start)
        assertEquals(400661, interval.end)
        assertEquals(22f, interval.score)

        interval = it.next()
        assertEquals(400621, interval.start)
        assertEquals(400671, interval.end)
        assertEquals(33f, interval.score)
        assertFalse(it.hasNext())
    }

    @Test fun testSplice() {
        section = FixedStepSection("chr1", 0)
        for (i in 0 until Short.MAX_VALUE * 2 - 100) {
            section.add(i.toFloat())
        }

        val subsections = section.splice().toList()
        assertEquals(2, subsections.size)
        assertEquals(0, subsections[0].start)
        assertEquals(Short.MAX_VALUE.toInt(), subsections[1].start)
    }

    @Test fun testSpliceRandom() {
        for (i in 0 until 100) {
            section = FixedStepSection("chr1", RANDOM.nextInt(99) + 1)
            val max = RANDOM.nextInt(99) + 1
            for (j in 0 until max * RANDOM.nextInt(99) + 1) {
                section.add(RANDOM.nextFloat())
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

class WigParserTest {
    @Test(expected = IllegalStateException::class) fun testInvalidType() {
        val input = "track type=wiggle_1 windowingFunction=mean\n"

        // Any type other than 'wiggle_0' should raise an IllegalStateException
        testWigSection(input)
    }

    @Test fun testVariableStep() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471   0.4242\n" + "10481   0.2424"

        testWigSection(input)
    }

    @Test fun testVariableStepNoSpan() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1\n" +
                    "10471   0.4242\n" +
                    "10481   0.2424"

        val it = WigParser(StringReader(input)).iterator()
        val track = it.next()

        assertNotNull(track)
        assertEquals("chr1", track.chrom)
        assertEquals(listOf(ScoredInterval(10470, 10471, 0.4242f),
                            ScoredInterval(10480, 10481, 0.2424f)),
                     track.query().toList())
    }

    @Test fun testFixedStep() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "fixedStep chrom=chr1 start=10471 step=10 span=5\n" +
                    "0.4242\n" +
                    "0.2424"

        testWigSection(input)
    }

    @Test fun testNaNInfinity() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "fixedStep chrom=chr1 start=10471 step=10 span=5\n" +
                    "NaN\n" +
                    "+Infinity\n" +
                    "-Infinity"

        val it = WigParser(input.reader()).iterator()
        val intervals = it.next().query().toList()
        assertTrue(intervals[0].score.isNaN())
        assertEquals(Float.POSITIVE_INFINITY, intervals[1].score)
        assertEquals(Float.NEGATIVE_INFINITY, intervals[2].score)
    }

    // Not implemented yet.
    @Test(expected = AssertionError::class) fun testFixedStepReuse() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "fixedStep chrom=chr1 start=10471 step=10 span=5\n" +
                    "0.4242\n" +
                    "fixedStep chrom=chr1 start=10475 step=10 span=5\n" +
                    "0.2424"

        // Both of the tracks should be merged, because one starts right
        // after another.
        testWigSection(input)
    }

    @Test fun testManyChromosomes() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471   0.4242\n" +
                    "variableStep chrom=chr2 span=5\n" +
                    "10481   0.2424\n" +
                    "variableStep chrom=chr3 span=5\n" +
                    "10481   0.2424\n"

        val it = WigParser(StringReader(input)).iterator()
        assertEquals(3, it.asSequence().count())
    }

    // Not implemented yet.
    @Test(expected = AssertionError::class) fun testVariableStepReuse() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471   0.4242\n" +
                    "variableStep chrom=chr1 span=5\n" + "10481   0.2424"

        // Both of the tracks should be merged, because they're on
        // the same chromosome.
        testWigSection(input)
    }

    @Test fun testNoReuseDifferentChromosomes() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471    0.4242\n" +
                    "fixedStep chrom=chr2 start=10475 step=10 span=5\n" +
                    "0.2424"

        val tracks = WigParser(StringReader(input)).toList()
        assertEquals(2, tracks.size)
        assertEquals("chr1", tracks[0].chrom)
        assertEquals("chr2", tracks[1].chrom)
    }

    @Test fun testNoReuseSameChromosome() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471    0.4242\n" +
                    "fixedStep chrom=chr1 start=10475 step=10 span=5\n" +
                    "0.2424"

        val tracks = WigParser(StringReader(input)).toList()
        assertEquals(2, tracks.size)
        assertEquals("chr1", tracks[0].chrom)
        assertEquals("chr1", tracks[1].chrom)
    }

    @Test fun testQuotedValues() {
        val input = "track type=wiggle_0 name=\"A sample track\" windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471   0.4242\n" +
                    "10481   0.2424"

        // We'll have an assertion error, if the parser fails.
        testWigSection(input)
    }

    @Test fun testDoubleExp() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471   1.01e+03\n" +
                    "10481   1e+03"

        val it = WigParser(StringReader(input)).iterator()
        assertTrue(it.hasNext())
        assertEquals(2, it.next().query().count())
    }

    private fun testWigSection(input: String) {
        val it = WigParser(StringReader(input)).iterator()
        val track = it.next()

        assertFalse(it.hasNext())
        assertNotNull(track)
        assertEquals("chr1", track.chrom)
        assertEquals(listOf(ScoredInterval(10470, 10475, 0.4242f),
                            ScoredInterval(10480, 10485, 0.2424f)),
                     track.query().toList())
    }
}

class WigPrinterTest {
    @Test fun testWriteFixedStep() {
        val track = FixedStepSection("chr1", 1000500)
        track.add(42f)
        track.add(24f)
        track.add(-42f)
        track.add(-24f)

        val output = StringWriter()
        WigPrinter(output, name = "yada").use { it.print(track) }

        val input = output.buffer.toString()
        val it = WigParser(StringReader(input)).iterator()
        val parsed = it.next()

        assertNotNull(parsed)
        assertFalse(it.hasNext())
        assertEquals("chr1", parsed.chrom)
        assertTrue(parsed is FixedStepSection)
        assertEquals(track, parsed)
    }

    @Test fun testWriteVariableStep() {
        val track = VariableStepSection("chr1")
        track[100500] = 42f
        track[500100] = 24f
        track[100100] = -42f
        track[500500] = -24f

        val output = StringWriter()
        WigPrinter(output, name = "yada").use { it.print(track) }

        val input = output.buffer.toString()
        val it = WigParser(StringReader(input)).iterator()
        val parsed = it.next()

        assertNotNull(parsed)
        assertFalse(it.hasNext())
        assertEquals("chr1", parsed.chrom)
        assertTrue(parsed is VariableStepSection)
        assertEquals(track, parsed)
    }
}