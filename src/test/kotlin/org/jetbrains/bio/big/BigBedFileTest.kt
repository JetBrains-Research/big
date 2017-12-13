package org.jetbrains.bio.big

import com.google.common.math.IntMath
import org.jetbrains.bio.*
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.awt.Color
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BigBedFileTest {
    @Test fun testQueryCompressed() = testQuery(Examples["example1-compressed.bb"])

    @Test fun testQueryUncompressed() = testQuery(Examples["example1.bb"])

    @Test fun bed9Format() {
        BigBedFile.read(Examples["bed9.bb"]).use { bb ->
            val items = bb.query("chr1").toList()
            assertEquals(ExtendedBedEntry("chr1", 0, 9800, "15_Quies", 0, '.',
                                          0, 9800, Color(255, 255, 255).rgb),
                         items.first().unpack())
            assertEquals(ExtendedBedEntry("chr1", 724000, 727200, "8_ZNF/Rpts", 0, '.',
                                          724000, 727200, Color(102, 205, 170).rgb),
                         items.last().unpack())
        }
    }

    private fun testQuery(path: Path) {
        val items = BedFile.read(Examples["example1.bed"]).toList()
        testQuerySmall(path, items)
        testQueryLarge(path, items)
    }

    private fun testQuerySmall(path: Path, items: List<BedEntry>) {
        BigBedFile.read(path).use { bbf ->
            for (i in 0 until 100) {
                testQuery(bbf, items, items[RANDOM.nextInt(items.size)])
            }
        }
    }

    private fun testQueryLarge(path: Path, items: List<BedEntry>) {
        BigBedFile.read(path).use { bbf ->
            for (i in 0 until 10) {
                val a = items[RANDOM.nextInt(items.size)]
                val b = items[RANDOM.nextInt(items.size)]
                testQuery(bbf, items, BedEntry(a.chrom, Math.min(a.start, b.start),
                                               Math.max(a.end, b.end)))
            }
        }
    }

    private fun testQuery(bbf: BigBedFile, items: List<BedEntry>, query: BedEntry) {
        val actual = bbf.query(query.chrom, query.start, query.end).toList()
        for (item in actual) {
            assertTrue(item.start >= query.start && item.end <= query.end)
        }

        val expected = items.asSequence()
                .filter { it.start >= query.start && it.end <= query.end }
                .toList()

        assertEquals(expected.size, actual.size);
        assertEquals(expected, actual, message = query.toString())
    }

    @Test fun testQueryConsistencyNoOverlaps() = testQueryConsistency(false)

    @Test fun testQueryConsistencyWithOverlaps() = testQueryConsistency(true)

    private fun testQueryConsistency(overlaps: Boolean) {
        BigBedFile.read(Examples["example1.bb"]).use { bbf ->
            val input = MMBRomBuffer(bbf.memBuff)
            val (name, chromIx, _/* size */) =
                    bbf.bPlusTree.traverse(input).first()
            val bedEntries = bbf.query(name).toList()
            val i = RANDOM.nextInt(bedEntries.size)
            val j = RANDOM.nextInt(bedEntries.size)
            val query = Interval(chromIx,
                                 bedEntries[Math.min(i, j)].start,
                                 bedEntries[Math.max(i, j)].end)
            for (bedEntry in bbf.query(name, query.startOffset, query.endOffset)) {
                val interval = Interval(chromIx, bedEntry.start, bedEntry.end)
                if (overlaps) {
                    assertTrue(interval intersects query)
                } else {
                    assertTrue(interval in query)
                }

            }
        }
    }

    @Test fun testAggregate() {
        assertEquals(emptyList<Pair<BedEntry, Int>>(), emptySequence<BedEntry>().aggregate())
        assertEquals(listOf(BedEntry("chr1", 0, 100, "") to 1),
                     sequenceOf(BedEntry("chr1", 0, 100)).aggregate())

        // Duplicate intervals.
        assertEquals(listOf(BedEntry("chr1", 0, 100, "") to 2),
                     sequenceOf(BedEntry("chr1", 0, 100),
                                BedEntry("chr1", 0, 100)).aggregate())
        // Consecutive intervals.
        assertEquals(listOf(BedEntry("chr1", 0, 100, "") to 2,
                            BedEntry("chr1", 100, 110, "") to 1),
                     sequenceOf(BedEntry("chr1", 0, 100),
                                BedEntry("chr1", 0, 110)).aggregate())
        // Disjoint intervals.
        assertEquals(listOf(BedEntry("chr1", 0, 100, "") to 1,
                            BedEntry("chr1", 200, 300, "") to 1),
                     sequenceOf(BedEntry("chr1", 0, 100),
                                BedEntry("chr1", 200, 300)).aggregate())
        // Shifted intervals.
        assertEquals(listOf(BedEntry("chr1", 100, 200, "") to 1,
                            BedEntry("chr1", 200, 300, "") to 1),
                     sequenceOf(BedEntry("chr1", 100, 200),
                                BedEntry("chr1", 200, 300)).aggregate())
    }

    @Test fun testSummarizeWholeFile() {
        val bbf = BigBedFile.read(Examples["example1.bb"])
        val name = bbf.chromosomes.valueCollection().first()
        val (expected) = bbf.summarize(name, 0, 0, numBins = 1, index = false)
        val (summary) = bbf.summarize(name, 0, 0, numBins = 1)

        // Because zoom levels smooth the data we can only make sure
        // that raw data estimate does not exceed the one reported
        // via index.
        assertTrue(summary.count >= expected.count)
        assertTrue(summary.sum >= expected.sum)
    }

    @Test fun testSummarizeNoOverlapsTwoBins() {
        var startOffset = RANDOM.nextInt(1000000)
        val bedEntries = (0..IntMath.pow(2, 16)).asSequence().map {
            val endOffset = startOffset + RANDOM.nextInt(999) + 1
            val entry = BedEntry("chr1", startOffset, endOffset)
            startOffset = endOffset + RANDOM.nextInt(100)
            entry
        }.toList().sortedBy { it.start }

        testSummarize(bedEntries, numBins = 2)
    }

    @Test fun testSummarizeFourBins() {
        val bedEntries = (0..IntMath.pow(2, 16)).asSequence().map {
            val startOffset = RANDOM.nextInt(1000000)
            val endOffset = startOffset + RANDOM.nextInt(999) + 1
            val entry = BedEntry("chr1", startOffset, endOffset)
            entry
        }.toList().sortedBy { it.start }

        testSummarize(bedEntries, numBins = 4)
    }

    @Test fun testSummarizeManyBins() {
        val bedEntries = (0..IntMath.pow(2, 16)).asSequence().map {
            val startOffset = RANDOM.nextInt(1000000)
            val endOffset = startOffset + RANDOM.nextInt(999) + 1
            val entry = BedEntry("chr1", startOffset, endOffset)
            entry
        }.toList().sortedBy { it.start }

        testSummarize(bedEntries, numBins = 1000)
    }

    @Test fun testConcurrentChrAccess() {
        BigFileTest.doTestConcurrentChrAccess("concurrent.bb",
                                              arrayOf("chr1" to 2657021, "chr2" to 2657021,
                                                      "chr3" to 2657021, "chr4" to 2657021))
    }
    
    @Test fun testConcurrentDataAccess() {
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
        
            BigFileTest.doTestConcurrentDataAccess("concurrent.bb", expected, true)
      }

    private fun testSummarize(bedEntries: List<BedEntry>, numBins: Int) {
        val name = bedEntries.map { it.chrom }.first()
        withTempFile("example", ".bb") { path ->
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"].chromosomes(), path)
            BigBedFile.read(path).use { bbf ->
                val aggregate = bedEntries.asSequence().aggregate()
                val summaries = bbf.summarize(name, 0, 0, numBins)
                assertEquals(aggregate.map { (e, _) -> e.end - e.start }.sum().toLong(),
                             summaries.map { it.count }.sum())

                assertEquals(aggregate.map { (e, cov) -> cov.toLong() * (e.end - e.start) }.sum(),
                             summaries.map { it.sum }.sum().toLong())
            }
        }
    }

    companion object {
        private val RANDOM = Random()
    }

    @Test fun testWriteReadZoomOverflow() {
        withTempFile("example1", ".bb") { path ->
            val bedEntries = BedFile.read(Examples["reduction_overflow.bed"]).toList()
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path,
                             zoomLevelCount = 20)

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries,
                             bbf.query("chr2", 0, 0).toList()
                                     + bbf.query("chr22", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
            }
        }
    }

    @Test fun testBedPlusBedAllFields() {
        withTempFile("example1", ".bb") { path ->
            val bedEntries = listOf(
                    ExtendedBedEntry("chr1", 1, 100, ".", 0, '+',
                                     15, 25,
                                     Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                                     extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")),
                    ExtendedBedEntry("chr1", 200, 300, ".", 800, '.',
                                     20, 22,
                                     0, 0,
                                     extraFields = arrayOf("193.07668", "-1.00000", "4.91755", "171"))
            ).map { it.pack() }
            BigBedFile.write(bedEntries,
                             Examples["hg19.chrom.sizes.gz"].chromosomes(), path)

            BigBedFile.read(path).use { bbf ->
                assertEquals(
                        bedEntries,
                        bbf.query("chr1", 0, 0).toList()
                )
                assertFalse(bbf.totalSummary.isEmpty())
            }
        }
    }

    @Test fun testBedPlusBed3p4() {
        withTempFile("example1", ".bb") { path ->
            val bedEntries = listOf(
                    BedEntry("chr1", 1, 100,
                             rest = "34.56398\t-1.00000\t4.91755\t240"),
                    BedEntry("chr1", 200, 300,
                             rest = "193.07668\t-1.00000\t4.91755\t171")
            )
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path
            )

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries, bbf.query("chr1", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
            }
        }
    }

    @Test fun testBedPlusBed6p4() {
        withTempFile("example1", ".bb") { path ->
            val bedEntries = BedFile.read(Examples["bed6p4.bed"]).toList()
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path
            )

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries,
                             bbf.query("chr1", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
            }
        }
    }
}

@RunWith(Parameterized::class)
class BigBedReadWriteTest(private val order: ByteOrder,
                          private val compression: CompressionType) {

    @Test fun testWriteReadSmall() {
        withTempFile("small", ".bb") { path ->
            val bedEntries = listOf(BedEntry("chr21", 0, 100))
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)
            BigBedFile.read(path).use { bbf ->
                assertEquals(1, bbf.query("chr21", 0, 0).count())
                assertEquals(bedEntries, bbf.query("chr21", 0, 0).toList())
            }
        }
    }

    @Test fun testWriteReadEmpty() {
        withTempFile("empty", ".bb") { path ->
            BigBedFile.write(emptyList<BedEntry>(),
                             Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)
            BigBedFile.read(path).use { bbf ->
                assertEquals(0, bbf.query("chr21", 0, 0).count())
            }
        }
    }

    @Test fun testWriteReadMultipleChromosomes() {
        withTempFile("empty", ".bb") { path ->
            BigBedFile.write(listOf(BedEntry("chr1", 100, 200), BedEntry("chr2", 50, 150)),
                             Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)
            BigBedFile.read(path).use { bbf ->
                assertEquals(0, bbf.query("chr21", 0, 0).count())
            }
        }
    }

    @Test fun testWriteReadMissingChromosome() {
        withTempFile("empty", ".bb") { path ->
            // In case of error this would raise an exception.
            BigBedFile.write(listOf(BedEntry("chr1", 100, 200), BedEntry("chr2", 50, 150)),
                             listOf("chr1" to 500100),
                             path, compression = compression, order = order)
        }
    }

    @Test fun testWriteRead() {
        withTempFile("example1", ".bb") { path ->
            val bedEntries = BedFile.read(Examples["example1.bed"]).toList()
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries, bbf.query("chr21", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
            }
        }
    }

    @Test fun testWriteReadBed12() {
        withTempFile("example.bed12", ".bb") { path ->
            val bedEntries = BedFile.read(Examples["example.bed12.bed"]).toList()
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries, bbf.query("chr21", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
                assertEquals(bedEntries.last().unpack(),
                             ExtendedBedEntry("chr21", 9480532, 9481699, "Neg4",
                                              0, '-', 9480532, 9481699,
                                              Color(0, 0, 255).rgb,
                                              2, intArrayOf(933,399), intArrayOf(0, 9601)))
            }
        }
    }

    @Test fun testWriteReadBed12NoColor() {
        withTempFile("bed12.nocolor", ".bb") { path ->
            val bedEntries = BedFile.read(Examples["bed12.nocolor.bed"]).toList()
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries, bbf.query("chr21", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
                assertEquals(bedEntries.last().unpack(),
                             ExtendedBedEntry("chr21", 2000, 6000, "cloneB",
                                              900, '-', 2000, 6000, 0,
                                              2, intArrayOf(433, 399), intArrayOf(0, 3601)))
            }
        }
    }

    @Test fun testWriteReadBed9() {
        withTempFile("example.bed9", ".bb") { path ->
            val bedEntries = BedFile.read(Examples["example.bed9.bed"]).toList()
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries, bbf.query("chr21", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
            }
        }
    }

    @Test fun testWriteReadBed6() {
        withTempFile("example", ".bb") { path ->
            val bedEntries = BedFile.read(Examples["example.bed6.bed"]).toList()
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries, bbf.query("chr21", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
            }
        }
    }

    @Test fun testWriteReadBed12As9() {
        withTempFile("example", ".bb") { path ->
            val bed12Entries = BedFile.read(Examples["example.bed12.bed"]).toList()
            val bedEntries = BedFile.read(Examples["example.bed9.bed"]).toList()
            BigBedFile.write(bed12Entries.map { it.unpack().pack(fieldsNumber = 9) },
                             Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries, bbf.query("chr21", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
            }
        }
    }

    @Test fun testWriteReadBed12As6() {
        withTempFile("example", ".bb") { path ->
            val bed12Entries = BedFile.read(Examples["example.bed12.bed"]).toList()
            val bedEntries = BedFile.read(Examples["example.bed6.bed"]).toList()
            BigBedFile.write(bed12Entries.map { it.unpack().pack(fieldsNumber = 6) },
                             Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries,
                             bbf.query("chr21", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
            }
        }
    }

    @Test fun testWriteReadBed12As3() {
        withTempFile("example", ".bb") { path ->
            val bed12Entries = BedFile.read(Examples["example.bed12.bed"]).toList()
            val bedEntries = BedFile.read(Examples["example.bed3.bed"]).toList()
            BigBedFile.write(bed12Entries.map { it.unpack().pack(fieldsNumber = 3) },
                             Examples["hg19.chrom.sizes.gz"].chromosomes(),
                             path, compression = compression, order = order)

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries, bbf.query("chr21", 0, 0).toList())
                assertFalse(bbf.totalSummary.isEmpty())
            }
        }
    }

    companion object {
        @Parameterized.Parameters(name = "{0}:{1}")
        @JvmStatic fun data(): Iterable<Array<Any>> {
            return listOf(
                    arrayOf(ByteOrder.BIG_ENDIAN, CompressionType.NO_COMPRESSION),
                    arrayOf(ByteOrder.BIG_ENDIAN, CompressionType.DEFLATE),
                    arrayOf(ByteOrder.BIG_ENDIAN, CompressionType.SNAPPY),
                    arrayOf(ByteOrder.LITTLE_ENDIAN, CompressionType.NO_COMPRESSION),
                    arrayOf(ByteOrder.LITTLE_ENDIAN, CompressionType.DEFLATE),
                    arrayOf(ByteOrder.LITTLE_ENDIAN, CompressionType.SNAPPY)
            )
        }
    }
}
