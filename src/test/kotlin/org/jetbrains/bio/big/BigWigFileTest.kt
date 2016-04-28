package org.jetbrains.bio.big

import org.apache.commons.math3.util.Precision
import org.jetbrains.bio.*
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BigWigFileTest {
    @Test fun testCompressedExample2() {
        assertVariableStep(Examples["example2.bw"],
                           "chr21", 9411191, 50f, 48119895, 60f)
    }

    @Test fun testVariableStep() {
        assertVariableStep(Examples["variable_step.bw"],
                           "chr2", 300701, 12.5f, 300705, 12.5f)
    }

    @Test fun testVariableStepWithSpan() {
        assertVariableStep(Examples["variable_step_with_span.bw"],
                           "chr2", 300701, 12.5f, 300705, 12.5f)
    }

    @Test fun testFixedStep() {
        assertFixedStep(Examples["fixed_step.bw"],
                        "chr3", 400601, 11f, 400801, 33f)
    }

    @Test fun testFixedStepWithSpan() {
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

    @Test fun testSummarizeWholeFile() {
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

    @Test fun testSummarizeSingleBpBins() {
        BigWigFile.read(Examples["example2.bw"]).use { bwf ->
            val name = bwf.chromosomes.valueCollection().first()
            val summaries = bwf.summarize(name, 0, 100, numBins = 100)
            assertEquals(100, summaries.size)
        }
    }

    @Test(expected = IllegalArgumentException::class) fun testSummarizeTooManyBins() {
        BigWigFile.read(Examples["example2.bw"]).use { bwf ->
            val name = bwf.chromosomes.valueCollection().first()
            bwf.summarize(name, 0, 100, numBins = 200)
        }
    }

    @Test fun testSummarizeFourBins() {
        val wigSections = (0 until 128).asSequence().map {
            val startOffset = RANDOM.nextInt(1000000)
            val section = FixedStepSection("chr1", startOffset)
            for (i in 0 until RANDOM.nextInt(127) + 1) {
                section.add(RANDOM.nextFloat())
            }

            section
        }.toList().sortedBy { it.start }

        testSummarize(wigSections, numBins = 4, index = false)
        testSummarize(wigSections, numBins = 4, index = true)
    }

    private fun testSummarize(wigSections: List<WigSection>, numBins: Int, index: Boolean) {
        val name = wigSections.map { it.chrom }.first()
        withTempFile("example", ".bw") { path ->
            BigWigFile.write(wigSections, Examples["hg19.chrom.sizes.gz"].chromosomes(), path)
            BigWigFile.read(path).use { bbf ->
                val summaries = bbf.summarize(name, 0, 0, numBins, index = index)
                val expected = wigSections.map { it.query().map { it.score }.sum() }.sum().toDouble()
                val actual = summaries.map { it.sum }.sum()
                assertTrue(Precision.equalsWithRelativeTolerance(expected, actual, 0.1),
                           "$expected /= $actual")
            }
        }
    }

    @Test fun testQueryPartialVariable() = testQueryPartial(Examples["example2.bw"])

    @Test fun testQueryPartialFixed() = testQueryPartial(Examples["fixed_step.bw"])

    @Test fun testQueryPartialBedGraph() {
        withTempFile("bed_graph", ".bw") { path ->
            BigWigFile.read(Examples["fixed_step.bw"]).use { bwf ->
                val name = bwf.chromosomes.valueCollection().first()
                BigWigFile.write(bwf.query(name).map { it.toBedGraph() }.toList(),
                                 Examples["hg19.chrom.sizes.gz"].chromosomes(),
                                 path)
            }

            testQueryPartial(path)
        }
    }

    @Test fun testQueryLeftEndAligned() {
        BigWigFile.read(Examples["fixed_step.bw"]).use { bwf ->
            assertEquals("FixedStepSection{chr3, start=400600, end=400801, step=100, span=1}",
                         bwf.query("chr3").first().toString())

            val expected = listOf(
                    ScoredInterval(400700, 400701, 22.0f),
                    ScoredInterval(400800, 400801, 33.0f)
            )
            assertEquals(expected, bwf.query("chr3", 400700, 410000)
                    .flatMap { it.query() }.toList())
        }
    }

    @Test fun testQueryRightEndAligned() {
        BigWigFile.read(Examples["fixed_step.bw"]).use { bwf ->
            assertEquals("FixedStepSection{chr3, start=400600, end=400801, step=100, span=1}",
                         bwf.query("chr3").first().toString())

            val expected = listOf(
                    ScoredInterval(400700, 400701, 22.0f),
                    ScoredInterval(400800, 400801, 33.0f)
            )
            assertEquals(expected, bwf.query("chr3", 400620, 400801)
                    .flatMap { it.query() }.toList())
        }
    }

    @Test fun testQueryInnerRange() {
        BigWigFile.read(Examples["fixed_step.bw"]).use { bwf ->
            assertEquals("FixedStepSection{chr3, start=400600, end=400801, step=100, span=1}",
                         bwf.query("chr3").first().toString())

            val expected = listOf(ScoredInterval(400700, 400701, 22.0f))
            assertEquals(expected, bwf.query("chr3", 400620, 400800)
                    .flatMap { it.query() }.toList())
        }
    }

    @Test fun testQueryOuterRange() {
        BigWigFile.read(Examples["fixed_step.bw"]).use { bwf ->
            assertEquals("FixedStepSection{chr3, start=400600, end=400801, step=100, span=1}",
                         bwf.query("chr3").first().toString())

            val expected = listOf(
                    ScoredInterval(400600, 400601, 11.0f),
                    ScoredInterval(400700, 400701, 22.0f),
                    ScoredInterval(400800, 400801, 33.0f)
            )
            assertEquals(expected, bwf.query("chr3", 400000, 410000)
                    .flatMap { it.query() }.toList())
        }
    }

    @Test fun testQueryWithOverlaps() = withTempFile("fixed_step", ".bw") { path ->
        val section = FixedStepSection("chr3", 400600, step = 100, span = 50)
        section.add(11.0f)
        section.add(22.0f)
        section.add(33.0f)

        BigWigFile.write(listOf(section), Examples["hg19.chrom.sizes.gz"].chromosomes(), path)
        BigWigFile.read(path).use { bwf ->
            assertEquals("FixedStepSection{chr3, start=400600, end=400850, step=100, span=50}",
                         bwf.query("chr3").first().toString())

            val expected = listOf(
                    ScoredInterval(400600, 400650, 11.0f),
                    ScoredInterval(400700, 400750, 22.0f),
                    ScoredInterval(400800, 400850, 33.0f)
            )

            assertEquals(expected,
                         bwf.query("chr3", 400600, 410000, overlaps = false)
                                 .flatMap { it.query() }.toList())
            assertEquals(expected,
                         bwf.query("chr3", 400615, 410000, overlaps = true)
                                 .flatMap { it.query() }.toList())
            assertEquals(expected.subList(1, expected.size),
                         bwf.query("chr3", 400715, 410000, overlaps = true)
                                 .flatMap { it.query() }.toList())
        }
    }

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
            assertEquals(expected.query().toList().subList(1, expected.size()),
                         bwf.query(name, end, expected.end).first().query().toList())
            assertEquals(expected.query().toList().subList(1, expected.size()),
                         bwf.query(name, end - (end - start) / 2,
                                   expected.end).first().query().toList())
        }
    }

    @Test fun testQueryConsistencyNoOverlaps() = testQueryConsistency(false)

    @Test fun testQueryConsistencyWithOverlaps() = testQueryConsistency(true)

    private fun testQueryConsistency(overlaps: Boolean) {
        BigWigFile.read(Examples["example2.bw"]).use { bwf ->
            val (name, chromIx, _size) = bwf.bPlusTree.traverse(bwf.input).first()
            val wigItems = bwf.query(name).flatMap { it.query().asSequence() }.toList()
            val i = RANDOM.nextInt(wigItems.size)
            val j = RANDOM.nextInt(wigItems.size)
            val query = Interval(chromIx,
                                 wigItems[Math.min(i, j)].start,
                                 wigItems[Math.max(i, j)].end)
            for (section in bwf.query(name, query.startOffset, query.endOffset)) {
                for (wigItem in section.query()) {
                    val interval = Interval(chromIx, wigItem.start, wigItem.end)
                    if (overlaps) {
                        assertTrue(interval intersects query)
                    } else {
                        assertTrue(interval in query)
                    }
                }
            }
        }
    }

    companion object {
        private val RANDOM = Random()

        private fun WigSection.toBedGraph(): BedGraphSection {
            val surrogate = BedGraphSection(chrom)
            for ((startOffset, endOffset, score) in query()) {
                surrogate[startOffset, endOffset] = score
            }

            return surrogate
        }
    }
}

@RunWith(Parameterized::class)
class BigWigReadWriteTest(private val order: ByteOrder,
                          private val compression: CompressionType) {

    @Test fun testWriteReadNoData() {
        withTempFile("empty", ".bw") { path ->
            BigWigFile.write(emptyList<WigSection>(),
                             Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)
            BigWigFile.read(path).use { bbf ->
                assertEquals(0, bbf.query("chr21", 0, 0).count())
            }
        }
    }

    @Test fun testWriteReadEmptySection() {
        withTempFile("empty", ".bw") { path ->
            BigWigFile.write(listOf(VariableStepSection("chr21")),
                             Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)
            BigWigFile.read(path).use { bbf ->
                assertEquals(0, bbf.query("chr21", 0, 0).count())
            }
        }
    }

    @Test fun testWriteReadMultipleChromosomes() {
        withTempFile("empty", ".bw") { path ->
            val section1 = VariableStepSection("chr19").apply {
                this[100500] = 42.0f
                this[100600] = 24.0f
            }
            val section2 = VariableStepSection("chr21").apply {
                this[500] = 42.0f
                this[600] = 24.0f
            }

            BigWigFile.write(listOf(section1, section2),
                             Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)
            BigWigFile.read(path).use { bbf ->
                assertEquals(1, bbf.query("chr19", 0, 0).count())
                assertEquals(1, bbf.query("chr21", 0, 0).count())
            }
        }
    }

    @Test fun testWriteReadMissingChromosome() {
        withTempFile("empty", ".bw") { path ->
            val section1 = VariableStepSection("chr19").apply {
                this[100500] = 42.0f
                this[100600] = 24.0f
            }
            val section2 = VariableStepSection("chr21").apply {
                this[500] = 42.0f
                this[600] = 24.0f
            }

            // In case of error this would raise an exception.
            BigWigFile.write(listOf(section1, section2),
                             listOf("chr19" to 500100),
                             path, compression = compression, order = order)
        }
    }

    @Test fun testWriteRead() {
        withTempFile("example", ".bw") { path ->
            val wigSections = WigFile(Examples["example.wig"]).toList()
            BigWigFile.write(wigSections, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)
            BigWigFile.read(path).use { bwf ->
                assertEquals(wigSections, bwf.query("chr19", 0, 0).toList())
                assertFalse(bwf.totalSummary.isEmpty())
            }
        }
    }

    companion object {
        @Parameterized.Parameters(name = "{0}:{1}")
        @JvmStatic fun data(): Iterable<Array<Any>> {
            return listOf(arrayOf(ByteOrder.BIG_ENDIAN, CompressionType.NO_COMPRESSION),
                          arrayOf(ByteOrder.BIG_ENDIAN, CompressionType.DEFLATE),
                          arrayOf(ByteOrder.BIG_ENDIAN, CompressionType.SNAPPY),
                          arrayOf(ByteOrder.LITTLE_ENDIAN, CompressionType.NO_COMPRESSION),
                          arrayOf(ByteOrder.LITTLE_ENDIAN, CompressionType.DEFLATE),
                          arrayOf(ByteOrder.LITTLE_ENDIAN, CompressionType.SNAPPY))
        }
    }
}
