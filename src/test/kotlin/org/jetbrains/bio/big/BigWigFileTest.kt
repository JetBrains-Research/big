package org.jetbrains.bio.big

import org.apache.commons.math3.util.Precision
import org.junit.Test
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

public class BigWigFileTest {
    Test fun testWriteReadCompressedBE() = testWriteRead(true, ByteOrder.BIG_ENDIAN)

    Test fun testWriteReadUncompressedBE() = testWriteRead(false, ByteOrder.BIG_ENDIAN)

    Test fun testWriteReadCompressedLE() = testWriteRead(true, ByteOrder.LITTLE_ENDIAN)

    Test fun testWriteReadUncompressedLE() = testWriteRead(false, ByteOrder.LITTLE_ENDIAN)

    private fun testWriteRead(compressed: Boolean, order: ByteOrder) {
        val path = Files.createTempFile("example", ".bw")
        try {
            val wigSections = WigParser(Examples["example.wig"].toFile().bufferedReader())
                    .map { it.second }
                    .toList()
            BigWigFile.write(wigSections, Examples["hg19.chrom.sizes.gz"],
                             path, compressed = compressed, order = order)

            BigWigFile.read(path).use { bwf ->
                assertEquals(wigSections, bwf.query("chr19", 0, 0).toList())
            }
        } finally {
            Files.deleteIfExists(path)
        }
    }

    Test fun testCompressedExample2() {
        assertVariableStep(Examples["example2.bw"],
                           "chr21", 9411191, 50f, 48119895, 60f)
    }

    Test fun testVariableStep() {
        assertVariableStep(Examples["variable_step.bw"],
                           "chr2", 300701, 12.5f, 300705, 12.5f)
    }

    Test fun testVariableStepWithSpan() {
        assertVariableStep(Examples["variable_step_with_span.bw"],
                           "chr2", 300701, 12.5f, 300705, 12.5f)
    }

    Test fun testFixedStep() {
        assertFixedStep(Examples["fixed_step.bw"],
                        "chr3", 400601, 11f, 400801, 33f)
    }

    Test fun testFixedStepWithSpan() {
        assertFixedStep(Examples["fixed_step_with_span.bw"],
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
        return BigWigFile.read(path).use { bwf ->
            val chromosomes = bwf.chromosomes

            assertEquals(1, chromosomes.size())
            assertEquals(chromosome, chromosomes.values().first())

            val steps = bwf.query(chromosome, 0, 0).toList()
            assertTrue(steps.isNotEmpty())
            steps
        }
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

    Test fun testSummarizeWholeFile() {
        BigWigFile.read(Examples["example2.bw"]).use { bwf ->
            val name = bwf.chromosomes.valueCollection().first()
            val (expected) = bwf.summarize(name, 0, 0, numBins = 1, index = false)
            val (summary) = bwf.summarize(name, 0, 0, numBins = 1)

            // Because zoom levels smooth the data we can only make sure
            // that raw data estimate does not exceed the one reported
            // via index.
            assertTrue(summary.count >= expected.count)
            assertTrue(summary.sum >= expected.sum)
        }
    }

    Test fun testSummarizeSingleBpBins() {
        BigWigFile.read(Examples["example2.bw"]).use { bwf ->
            val name = bwf.chromosomes.valueCollection().first()
            val summaries = bwf.summarize(name, 0, 100, numBins = 100)
            assertEquals(100, summaries.size())
        }
    }

    Test(expected = IllegalArgumentException::class) fun testSummarizeTooManyBins() {
        BigWigFile.read(Examples["example2.bw"]).use { bwf ->
            val name = bwf.chromosomes.valueCollection().first()
            bwf.summarize(name, 0, 100, numBins = 200)
        }
    }

    Test fun testSummarizeFourBins() {
        val wigSections = (0 until 128).asSequence().map {
            var startOffset = RANDOM.nextInt(1000000)
            val section = FixedStepSection("chr1", startOffset)
            for (i in 0 until RANDOM.nextInt(127) + 1) {
                section.add(RANDOM.nextFloat())
            }

            section
        }.toList().sortBy { it.start }

        testSummarize(wigSections, numBins = 4)
    }

    private fun testSummarize(wigSections: List<WigSection>, numBins: Int) {
        val name = wigSections.map { it.chrom }.first()
        val path = Files.createTempFile("example", ".bw")
        try {
            BigWigFile.write(wigSections, Examples["hg19.chrom.sizes.gz"], path)
            BigWigFile.read(path).use { bbf ->
                val summaries = bbf.summarize(name, 0, 0, numBins)
                assertTrue(Precision.equals(
                        wigSections.map { it.query().map { it.score }.sum() }.sum().toDouble(),
                        summaries.map { it.sum }.sum(), 0.1))
            }
        } finally {
            Files.deleteIfExists(path)
        }
    }

    Test fun testQueryPartialVariable() = testQueryPartial(Examples["example2.bw"])

    Test fun testQueryPartialFixed() = testQueryPartial(Examples["fixed_step.bw"])

    private fun testQueryPartial(path: Path) {
        BigWigFile.read(path).use { bwf ->
            val name = bwf.chromosomes.valueCollection().first()
            val expected = bwf.query(name, 0, 0).first()
            assertEquals(expected,
                         bwf.query(name, expected.start, expected.end).first())

            // omit first interval.
            val (start, end, _score) = expected.query().first()
            assertEquals(expected.size() - 1,
                         bwf.query(name, end, expected.end).first().size())
            assertEquals(expected.query().subList(1, expected.size()),
                         bwf.query(name, end, expected.end).first().query())
            assertEquals(expected.query().subList(1, expected.size()),
                         bwf.query(name, end - (end - start) / 2,
                                   expected.end).first().query())
        }
    }

    Test fun testTrimmedQuery() {
        val query = Interval(0, 56955102, 57854868)
        val numBins = 1592
        val slices = query.truncatedSlice1(listOf(WigInterval(56955200, 56955300, 0.0f),
                                                  WigInterval(57854700, 57854800, 0.0f)),
                                           numBins)
                .toList()
        assertTrue(slices.size() <= numBins)
        for (slice in slices) {
            assertTrue(slice in query)
        }
    }

    companion object {
        private val RANDOM = Random()
    }
}