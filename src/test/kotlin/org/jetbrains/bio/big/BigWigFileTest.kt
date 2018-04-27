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
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@RunWith(Parameterized::class)
class BigWigFileTest(
        private val bfProvider: NamedRomBufferFactoryProvider,
        private val prefetch: Int
) {
    @Test
    fun header() {
        BigWigFile.read(Examples["example2.bw"], bfProvider, prefetch).use { bf ->
            assertEquals(10, bf.zoomLevels.size)
            assertEquals(25600, bf.zoomLevels[4].reduction)
            assertEquals(prefetch == BigFile.PREFETCH_LEVEL_OFF, bf.prefetchedLevel2RTreeIndex == null)
            if (prefetch >= BigFile.PREFETCH_LEVEL_FAST) {
                assertEquals(10, bf.prefetchedLevel2RTreeIndex!!.size)

                val zRTree0 = bf.prefetchedLevel2RTreeIndex!![bf.zoomLevels[0]]!!
                assertTrue(zRTree0.rootNode!! is RTReeNodeIntermediate)
                val zRTree0Node = zRTree0.rootNode!! as RTReeNodeIntermediate
                assertEquals(2, zRTree0Node.children.size)
                assertEquals(ChromosomeInterval(0, 39125325, 48119960),
                             zRTree0Node.children[1].interval)

                val zRTree4 = bf.prefetchedLevel2RTreeIndex!![bf.zoomLevels[4]]!!
                assertEquals(20049226, zRTree4.header.rootOffset)
                assertEquals(1379, zRTree4.header.itemCount)
                assertTrue(zRTree4.rootNode!! is RTReeNodeLeaf)
                assertEquals(ChromosomeInterval(0, 38965325, 48129895),
                             (zRTree4.rootNode!! as RTReeNodeLeaf).leaves[1].interval)
            }

            assertEquals(prefetch == BigFile.PREFETCH_LEVEL_OFF, bf.prefetchedChr2Leaf == null)
            if (prefetch >= BigFile.PREFETCH_LEVEL_FAST) {
                assertEquals(1, bf.prefetchedChr2Leaf!!.size)
                assertEquals("chr21", bf.prefetchedChr2Leaf!!["chr21"]!!.key)
                assertEquals(0, bf.prefetchedChr2Leaf!!["chr21"]!!.id)
                assertEquals(48129895, bf.prefetchedChr2Leaf!!["chr21"]!!.size)
            }

            assertEquals(35106705, bf.totalSummary.count)
            assertTrue(Precision.equals(1.433496108E9, bf.totalSummary.sum))

            assertEquals(listOf("chr21"), bf.chromosomes.valueCollection().toList())
            assertEquals(376, bf.bPlusTree.header.rootOffset)
            assertEquals(1, bf.bPlusTree.header.itemCount)

            assertEquals(15751097, bf.rTree.header.rootOffset)
            assertEquals(6857, bf.rTree.header.itemCount)
            if (prefetch >= BigFile.PREFETCH_LEVEL_DETAILED) {
                assertNotNull(bf.rTree.rootNode)
                assertTrue(bf.rTree.rootNode!! is RTReeNodeIntermediate)
                assertEquals(27, (bf.rTree.rootNode!! as RTReeNodeIntermediate).children.size)
                assertEquals("0:[9411190; 11071890)",
                        (bf.rTree.rootNode!! as RTReeNodeIntermediate).children[0].interval.toString())
                assertTrue((bf.rTree.rootNode!! as RTReeNodeIntermediate).children[0].node is RTreeNodeRef)
            }
        }
    }


    @Test
    fun testCompressedExample2() {
        assertVariableStep(Examples["example2.bw"],
                           "chr21", 9411191, 50f, 48119895, 60f)
    }

    @Test
    fun testVariableStep() {
        assertVariableStep(Examples["variable_step.bw"],
                           "chr2", 300701, 12.5f, 300705, 12.5f)
    }

    @Test
    fun testVariableStepWithSpan() {
        assertVariableStep(Examples["variable_step_with_span.bw"],
                           "chr2", 300701, 12.5f, 300705, 12.5f)
    }

    @Test
    fun testFixedStep() {
        assertFixedStep(Examples["fixed_step.bw"],
                        "chr3", 400601, 11f, 400801, 33f)
    }

    @Test
    fun testFixedStepWithSpan() {
        assertFixedStep(Examples["fixed_step_with_span.bw"],
                        "chr3", 400601, 11f, 400805, 33f)
    }

    private fun assertVariableStep(path: Path, chromosome: String,
                                   position1: Int, value1: Float,
                                   position2: Int, value2: Float) {
        val steps = readAndCheckChromosome(path, chromosome)
        assertVariableStep(steps.first(), steps.last(),
                           position1, value1, position2, value2)
    }

    private fun assertFixedStep(path: Path, chromosome: String,
                                position1: Int, value1: Float,
                                position2: Int, value2: Float) {
        val steps = readAndCheckChromosome(path, chromosome)
        assertFixedStep(steps.first(), steps.last(),
                        position1, value1, position2, value2)
    }

    private fun readAndCheckChromosome(path: Path, chromosome: String) =
            BigWigFile.read(path, bfProvider, prefetch).use { bwf ->
                val chromosomes = bwf.chromosomes

                assertEquals(1, chromosomes.size())
                assertEquals(chromosome, chromosomes.values().first())

                val steps = bwf.query(chromosome, 0, 0)
                assertTrue(steps.isNotEmpty())
                steps
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

    @Test
    fun testSummarizeWholeFile() {
        BigWigFile.read(Examples["example2.bw"], bfProvider, prefetch).use { bwf ->
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

    @Test
    fun testSummarizeSingleBpBins() {
        BigWigFile.read(Examples["example2.bw"], bfProvider, prefetch).use { bwf ->
            val name = bwf.chromosomes.valueCollection().first()
            val summaries = bwf.summarize(name, 0, 100, numBins = 100)
            assertEquals(100, summaries.size)
        }
    }

    @Test(expected = IllegalArgumentException::class)
    fun testSummarizeTooManyBins() {
        BigWigFile.read(Examples["example2.bw"], bfProvider, prefetch).use { bwf ->
            val name = bwf.chromosomes.valueCollection().first()
            bwf.summarize(name, 0, 100, numBins = 200)
        }
    }

    @Test
    fun testSummarizeFourBins() {
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
            BigWigFile.read(path, bfProvider, prefetch).use { bbf ->
                val summaries = bbf.summarize(name, 0, 0, numBins, index = index)
                val expected = wigSections.map { it.query().map { it.score }.sum() }.sum().toDouble()
                val actual = summaries.map { it.sum }.sum()
                assertTrue(Precision.equalsWithRelativeTolerance(expected, actual, 0.1),
                           "$expected /= $actual")
            }
        }
    }

    @Test
    fun testQueryPartialVariable() = testQueryPartial(Examples["example2.bw"])

    @Test
    fun testQueryPartialFixed() = testQueryPartial(Examples["fixed_step.bw"])

    @Test
    fun testQueryPartialBedGraph() {
        withTempFile("bed_graph", ".bw") { path ->
            BigWigFile.read(Examples["fixed_step.bw"], bfProvider, prefetch).use { bwf ->
                val name = bwf.chromosomes.valueCollection().first()
                BigWigFile.write(bwf.query(name).map { it.toBedGraph() },
                                 Examples["hg19.chrom.sizes.gz"].chromosomes(),
                                 path)
            }

            testQueryPartial(path)
        }
    }

    @Test
    fun testQueryLeftEndAligned() {
        BigWigFile.read(Examples["fixed_step.bw"], bfProvider, prefetch).use { bwf ->
            assertEquals("FixedStepSection{chr3, start=400600, end=400801, step=100, span=1}",
                         bwf.query("chr3").first().toString())

            val expected = listOf(
                    ScoredInterval(400700, 400701, 22.0f),
                    ScoredInterval(400800, 400801, 33.0f)
            )
            assertEquals(expected, bwf.query("chr3", 400700, 410000)
                    .flatMap { it.query().toList() })
        }
    }

    @Test
    fun testQueryRightEndAligned() {
        BigWigFile.read(Examples["fixed_step.bw"], bfProvider, prefetch).use { bwf ->
            assertEquals("FixedStepSection{chr3, start=400600, end=400801, step=100, span=1}",
                         bwf.query("chr3").first().toString())

            val expected = listOf(
                    ScoredInterval(400700, 400701, 22.0f),
                    ScoredInterval(400800, 400801, 33.0f)
            )
            assertEquals(expected, bwf.query("chr3", 400620, 400801)
                    .flatMap { it.query().toList() })
        }
    }

    @Test
    fun testQueryInnerRange() {
        BigWigFile.read(Examples["fixed_step.bw"], bfProvider, prefetch).use { bwf ->
            assertEquals("FixedStepSection{chr3, start=400600, end=400801, step=100, span=1}",
                         bwf.query("chr3").first().toString())

            val expected = listOf(ScoredInterval(400700, 400701, 22.0f))
            assertEquals(expected, bwf.query("chr3", 400620, 400800)
                    .flatMap { it.query().toList() })
        }
    }

    @Test
    fun testQueryOuterRange() {
        BigWigFile.read(Examples["fixed_step.bw"], bfProvider, prefetch).use { bwf ->
            assertEquals("FixedStepSection{chr3, start=400600, end=400801, step=100, span=1}",
                         bwf.query("chr3").first().toString())

            val expected = listOf(
                    ScoredInterval(400600, 400601, 11.0f),
                    ScoredInterval(400700, 400701, 22.0f),
                    ScoredInterval(400800, 400801, 33.0f)
            )
            assertEquals(expected, bwf.query("chr3", 400000, 410000)
                    .flatMap { it.query().toList() })
        }
    }

    @Test
    fun testQueryWithOverlaps() = withTempFile("fixed_step", ".bw") { path ->
        val section = FixedStepSection("chr3", 400600, step = 100, span = 50)
        section.add(11.0f)
        section.add(22.0f)
        section.add(33.0f)

        BigWigFile.write(listOf(section), Examples["hg19.chrom.sizes.gz"].chromosomes(), path)
        BigWigFile.read(path, bfProvider, prefetch).use { bwf ->
            assertEquals("FixedStepSection{chr3, start=400600, end=400850, step=100, span=50}",
                         bwf.query("chr3").first().toString())

            val expected = listOf(
                    ScoredInterval(400600, 400650, 11.0f),
                    ScoredInterval(400700, 400750, 22.0f),
                    ScoredInterval(400800, 400850, 33.0f)
            )

            assertEquals(expected,
                         bwf.query("chr3", 400600, 410000, overlaps = false)
                                 .flatMap { it.query().toList() })
            assertEquals(expected,
                         bwf.query("chr3", 400615, 410000, overlaps = true)
                                 .flatMap { it.query().toList() })
            assertEquals(expected.subList(1, expected.size),
                         bwf.query("chr3", 400715, 410000, overlaps = true)
                                 .flatMap { it.query().toList() })
        }
    }

    private fun testQueryPartial(path: Path) {
        BigWigFile.read(path, bfProvider, prefetch).use { bwf ->
            val name = bwf.chromosomes.valueCollection().first()
            val expected = bwf.query(name, 0, 0).first()
            assertEquals(expected,
                         bwf.query(name, expected.start, expected.end).first())

            // omit first interval.
            val (start, end, _/* score */) = expected.query().first()
            assertEquals(expected.size - 1,
                         bwf.query(name, end, expected.end).first().size)
            assertEquals(expected.query().toList().subList(1, expected.size),
                         bwf.query(name, end, expected.end).first().query().toList())
            assertEquals(expected.query().toList().subList(1, expected.size),
                         bwf.query(name, end - (end - start) / 2,
                                   expected.end).first().query().toList())
        }
    }

    @Test
    fun testQueryConsistencyNoOverlaps() = testQueryConsistency(false)

    @Test
    fun testQueryConsistencyWithOverlaps() = testQueryConsistency(true)

    private fun testQueryConsistency(overlaps: Boolean) {
        BigWigFile.read(Examples["example2.bw"], bfProvider, prefetch).use { bwf ->
            bwf.buffFactory.create().use { input ->
                val (name, chromIx, _/* size */) = bwf.bPlusTree.traverse(input).first()
                val wigItems = bwf.query(name).asSequence().flatMap { it.query().asSequence() }.toList()
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

        @Parameterized.Parameters(name = "{0}")
        @JvmStatic
        fun data() = romFactoryProviderAndPrefetchParams()
    }
}

@RunWith(Parameterized::class)
class BigWigFileConcurrencyTest(private val bfProvider: NamedRomBufferFactoryProvider,
                                private val prefetch: Int) {
    @Test
    fun testConcurrentChrAccess() {

        BigFileTest.doTestConcurrentChrAccess("concurrent.bw",
                                              arrayOf("chr1" to 2657021, "chr2" to 2657021,
                                                      "chr3" to 2657021, "chr4" to 2657021),
                                              bfProvider, prefetch, false)
    }

    @Test
    fun testConcurrentDataAccess() {
        val expected = arrayOf(
                0 to 490, 1 to 2095, 2 to 4082, 3 to 0, 4 to 0, 5 to 0, 6 to 0, 7 to 0, 8 to 0,
                9 to 2276, 10 to 2139, 11 to 7868, 12 to 8188, 13 to 5438, 14 to 3658, 15 to 4461,
                16 to 2956, 17 to 7364, 18 to 5494, 19 to 5456, 20 to 4908, 21 to 2580, 22 to 3588,
                23 to 6187, 24 to 5521, 25 to 5023, 26 to 4243, 27 to 2769, 28 to 2797, 29 to 4430,
                30 to 3973, 31 to 2080, 32 to 3384, 33 to 5515, 34 to 14301, 35 to 7841, 36 to 8267,
                37 to 4391, 38 to 5628, 39 to 4155, 40 to 11800, 41 to 5630, 42 to 8815, 43 to 10814,
                44 to 8783, 45 to 7916, 46 to 15045, 47 to 248525, 48 to 491778, 49 to 477901,
                50 to 425092, 51 to 12275, 52 to 7334, 53 to 5090, 54 to 8476, 55 to 13496,
                56 to 11233, 57 to 16723, 58 to 10054, 59 to 12596, 60 to 181465, 61 to 316541,
                62 to 5772, 63 to 3880, 64 to 7227, 65 to 14748, 66 to 13244, 67 to 13383,
                68 to 13577, 69 to 8804, 70 to 25613, 71 to 20331, 72 to 12898, 73 to 13131,
                74 to 12612, 75 to 14219, 76 to 6654, 77 to 0, 78 to 0, 79 to 0, 80 to 0, 81 to 0,
                82 to 0, 83 to 0, 84 to 0, 85 to 0, 86 to 0, 87 to 0, 88 to 0, 89 to 0, 90 to 0,
                91 to 0, 92 to 0, 93 to 0, 94 to 0, 95 to 0, 96 to 0, 97 to 0, 98 to 0, 99 to 0)
        BigFileTest.doTestConcurrentDataAccess("concurrent.bw", expected, bfProvider, prefetch,
                                               false)
    }

    companion object {
        @Parameterized.Parameters(name = "{0}:{1}")
        @JvmStatic
        fun data() = listOf(threadSafeRomFactoryProvidersAndPrefetchParams()[5])
//        @JvmStatic fun data() = threadSafeRomFactoryProvidersAndPrefetchParams()
    }
}

@RunWith(Parameterized::class)
class BigWigReadWriteTest(private val order: ByteOrder,
                          private val compression: CompressionType,
                          private val bfProvider: NamedRomBufferFactoryProvider,
                          private val prefetch: Int) {

    @Test
    fun testWriteReadNoData() {
        withTempFile("empty", ".bw") { path ->
            BigWigFile.write(emptyList<WigSection>(),
                             Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)
            BigWigFile.read(path, bfProvider, prefetch).use { bbf ->
                assertEquals(0, bbf.query("chr21", 0, 0).count())
            }
        }
    }

    @Test
    fun testWriteReadEmptySection() {
        withTempFile("empty", ".bw") { path ->
            BigWigFile.write(listOf(VariableStepSection("chr21")),
                             Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)
            BigWigFile.read(path, bfProvider, prefetch).use { bbf ->
                assertEquals(0, bbf.query("chr21", 0, 0).count())
            }
        }
    }

    @Test
    fun testWriteReadMultipleChromosomes() {
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
            BigWigFile.read(path, bfProvider, prefetch).use { bbf ->
                assertEquals(1, bbf.query("chr19", 0, 0).count())
                assertEquals(1, bbf.query("chr21", 0, 0).count())
            }
        }
    }

    @Test
    fun testWriteReadMissingChromosome() {
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

    @Test
    fun testWriteRead() {
        withTempFile("example", ".bw") { path ->
            val wigSections = WigFile(Examples["example.wig"]).toList()
            BigWigFile.write(wigSections, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)
            BigWigFile.read(path, bfProvider, prefetch).use { bwf ->
                assertEquals(wigSections, bwf.query("chr19", 0, 0))
                assertFalse(bwf.totalSummary.isEmpty())
            }
        }
    }

    companion object {
        @Parameterized.Parameters(name = "{0}:{1}:{2}:{3}")
        @JvmStatic
        fun data(): Iterable<Array<Any>> = allBigFileParams()
    }
}
