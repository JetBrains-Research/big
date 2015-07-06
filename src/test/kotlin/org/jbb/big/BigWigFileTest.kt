package org.jbb.big

import org.junit.Before
import org.junit.Test
import java.io.StringReader
import java.io.StringWriter
import java.nio.file.Path
import java.util.Arrays
import kotlin.properties.Delegates
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

public class BigWigTest {
    Test fun testCompressedExample2() {
        assertVariableStep(Examples.get("example2.bw"),
                           "chr21", 9411191, 50f, 48119895, 60f)
    }

    Test fun testVariableStep() {
        assertVariableStep(Examples.get("variable_step.bw"),
                           "chr2", 300701, 12.5f, 300705, 12.5f)
    }

    Test fun testVariableStepWithSpan() {
        assertVariableStep(Examples.get("variable_step_with_span.bw"),
                           "chr2", 300701, 12.5f, 300705, 12.5f)
    }

    Test fun testFixedStep() {
        assertFixedStep(Examples.get("fixed_step.bw"),
                        "chr3", 400601, 11f, 400801, 33f)
    }

    Test fun testFixedStepWithSpan() {
        assertFixedStep(Examples.get("fixed_step_with_span.bw"),
                        "chr3", 400601, 11f, 400805, 33f)
    }

    private fun assertVariableStep(path: Path, chromosome: String,
                                   position1: Int, value1: Float,
                                   position2: Int, value2: Float) {
        val steps = assertChromosome(path, chromosome)
        assertVariableStep(steps.first(), steps.last(),
                           position1, value1, position2, value2)
    }

    private fun assertFixedStep(path: Path, chromosome: String,
                                position1: Int, value1: Float,
                                position2: Int, value2: Float) {
        val steps = assertChromosome(path, chromosome)
        assertFixedStep(steps.first(), steps.last(),
                        position1, value1, position2, value2)
    }

    private fun assertChromosome(path: Path, chromosome: String): List<WigSection> {
        val file = BigWigFile.read(path)
        val chromosomes = file.chromosomes

        assertEquals(1, chromosomes.size())
        assertEquals(chromosome, chromosomes.first())

        val steps = file.query(chromosome, 0, 0).toList()
        assertTrue(steps.isNotEmpty())

        return steps
    }

    private fun assertVariableStep(firstStep: WigSection, lastStep: WigSection,
                                   position1: Int, value1: Float,
                                   position2: Int, value2: Float) {
        assertTrue(firstStep is VariableStepSection)
        assertTrue(lastStep is VariableStepSection)

        assertEquals(position1, firstStep.start + 1)
        assertEquals(value1, firstStep.query().first().score)
        assertEquals(position2, lastStep.end)
        assertEquals(value2, lastStep.query().last().score)
    }

    private fun assertFixedStep(firstStep: WigSection, lastStep: WigSection,
                                position1: Int, value1: Float,
                                position2: Int, value2: Float) {
        assertTrue(firstStep is FixedStepSection)
        assertTrue(lastStep is FixedStepSection)

        assertEquals(position1, firstStep.start + 1)
        assertEquals(value1, firstStep.query().first().score)
        assertEquals(position2, lastStep.end)
        assertEquals(value2, lastStep.query().last().score)
    }
}

public class FixedStepSectionTest {
    private var section: FixedStepSection by Delegates.notNull()

    Before fun setUp() {
        section = FixedStepSection("chr1", 400601, 100, 5)
        section.add(11f)
        section.add(22f)
        section.add(33f)
    }

    Test fun testGet() {
        assertEquals(11f, section[400601])
        assertEquals(22f, section[400701])
        assertEquals(33f, section[400801])
    }

    Test(expected = IndexOutOfBoundsException::class) fun testOutOfBounds() {
        section[400901]
    }

    Test fun testQueryNoBounds() {
        val it = section.query().iterator()
        var interval: WigInterval
        interval = it.next()
        assertEquals(400601, interval.startOffset)
        assertEquals(400606, interval.endOffset)
        assertEquals(11f, interval.score)

        interval = it.next()
        assertEquals(400701, interval.startOffset)
        assertEquals(400706, interval.endOffset)
        assertEquals(22f, interval.score)

        interval = it.next()
        assertEquals(400801, interval.startOffset)
        assertEquals(400806, interval.endOffset)
        assertEquals(33f, interval.score)
    }

    Test fun testQuery() {
        // Note: the value '400600' will be rounded to the nearest
        // interval, in this case to '400601'.
        val it = section.query(400600, 400607).iterator()
        assertNotNull(it.next())
        assertFalse(it.hasNext())
    }
}

public class VariableStepSectionTest {
    private var section: VariableStepSection by Delegates.notNull()

    Before fun setUp() {
        section = VariableStepSection("chr1", 20)
        section[100500] = 42.0f
        section[500100] = 24.0f
    }

    Test fun testQueryNoBounds() {
        val correct = arrayOf(WigInterval(100500, 100520, 42f),
                              WigInterval(500100, 500120, 24f))

        assertEquals(Arrays.asList(*correct), section.query())
    }

    Test fun testQuery() {
        // Add a few ranges from both sides.
        section[100100] = 4242.0f
        section[500500] = 2424.0f

        val correct = arrayOf(WigInterval(100500, 100520, 42f),
                              WigInterval(500100, 500120, 24f))

        // Note: why '500200'? because the 'end' value corresponds
        // to the right border of the rightmost 'Range', i. e.:
        //                 end
        //                  V
        // |----|----|----|----|----|
        //           +----+
        //       rightmost range
        assertEquals(Arrays.asList(*correct), section.query(100500, 500200))
    }

    Test fun testSet() {
        section[100500] = 42.0f
        assertEquals(42f + 42f , section[100500])
    }
}

public class WigParserTest {
    Test(expected = IllegalStateException::class) fun testInvalidType() {
        val input = "track type=wiggle_1 windowingFunction=mean\n"

        // Any type other than 'wiggle_0' should raise an IllegalStateException
        testGenomeTrack(input)
    }

    Test fun testVariableStep() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471   0.4242\n" + "10481   0.2424"

        testGenomeTrack(input)
    }

    Test fun testVariableStepNoSpan() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1\n" +
                    "10471   0.4242\n" +
                    "10481   0.2424"

        val it = WigParser(StringReader(input)).iterator()
        val pair = it.next()

        assertNotNull(pair)
        assertEquals("chr1", pair.first)
        assertEquals(arrayListOf(WigInterval(10470, 10471, 0.4242f),
                                 WigInterval(10480, 10481, 0.2424f)),
                     pair.second.query())
    }

    Test fun testFixedStep() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "fixedStep chrom=chr1 start=10471 step=10 span=5\n" +
                    "0.4242\n" + "0.2424"

        testGenomeTrack(input)
    }

    // Not implemented yet.
    Test(expected = AssertionError::class) fun testFixedStepReuse() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "fixedStep chrom=chr1 start=10471 step=10 span=5\n" +
                    "0.4242\n" +
                    "fixedStep chrom=chr1 start=10475 step=10 span=5\n" +
                    "0.2424"

        // Both of the tracks should be merged, because one starts right
        // after another.
        testGenomeTrack(input)
    }

    Test fun testManyChromosomes() {
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
    Test(expected = AssertionError::class) fun testVariableStepReuse() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471   0.4242\n" +
                    "variableStep chrom=chr1 span=5\n" + "10481   0.2424"

        // Both of the tracks should be merged, because they're on
        // the same chromosome.
        testGenomeTrack(input)
    }

    Test fun testNoReuseDifferentChromosomes() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471    0.4242\n" +
                    "fixedStep chrom=chr2 start=10475 step=10 span=5\n" +
                    "0.2424"

        val tracks = WigParser(StringReader(input)).toList()
        assertEquals(2, tracks.size())
        assertEquals("chr1", tracks[0].first)
        assertEquals("chr2", tracks[1].first)
    }

    Test fun testNoReuseSameChromosome() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471    0.4242\n" +
                    "fixedStep chrom=chr1 start=10475 step=10 span=5\n" +
                    "0.2424"

        val tracks = WigParser(StringReader(input)).toList()
        assertEquals(2, tracks.size())
        assertEquals("chr1", tracks[0].first)
        assertEquals("chr1", tracks[1].first)
    }

    Test fun testQuotedValues() {
        val input = "track type=wiggle_0 name=\"A sample track\" windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471   0.4242\n" +
                    "10481   0.2424"

        // We'll have an assertion error, if the parser fails.
        testGenomeTrack(input)
    }

    Test fun testDoubleExp() {
        val input = "track type=wiggle_0 windowingFunction=mean\n" +
                    "variableStep chrom=chr1 span=5\n" +
                    "10471   1.01e+03\n" +
                    "10481   1e+03"

        val it = WigParser(StringReader(input)).iterator()
        assertTrue(it.hasNext())
        assertEquals(2, it.next().second.query().size())

    }

    private fun testGenomeTrack(input: String) {
        val it = WigParser(StringReader(input)).iterator()
        val pair = it.next()

        assertFalse(it.hasNext())

        assertNotNull(pair)
        assertEquals("chr1", pair.first)

        assertEquals(arrayListOf(WigInterval(10470, 10475, 0.4242f),
                                 WigInterval(10480, 10485, 0.2424f)),
                     pair.second.query())
    }
}

public class WigPrinterTest {
    Test fun testWriteFixedStep() {
        val track = FixedStepSection("chr1", 1000500)
        track.add(42f)
        track.add(24f)
        track.add(-42f)
        track.add(-24f)

        val output = StringWriter()
        WigPrinter(output, name = "yada").use { it.print(track) }

        val input = output.getBuffer().toString()
        val it = WigParser(StringReader(input)).iterator()
        val pair = it.next()

        assertNotNull(pair)
        assertFalse(it.hasNext())
        assertEquals("chr1", pair.first)
        assertTrue(pair.second is FixedStepSection)
        assertEquals(track, pair.second)
    }

    Test fun testWriteVariableStep() {
        val track = VariableStepSection("chr1")
        track[100500] = 42f
        track[500100] = 24f
        track[100100] = -42f
        track[500500] = -24f

        val output = StringWriter()
        WigPrinter(output, name = "yada").use { it.print(track) }

        val input = output.getBuffer().toString()
        val it = WigParser(StringReader(input)).iterator()
        val pair = it.next()

        assertNotNull(pair)
        assertFalse(it.hasNext())
        assertEquals("chr1", pair.first)
        assertTrue(pair.second is VariableStepSection)
        assertEquals(track, pair.second)
    }
}