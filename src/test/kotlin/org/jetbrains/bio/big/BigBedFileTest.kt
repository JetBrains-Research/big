package org.jetbrains.bio.big

import org.apache.commons.math3.util.Precision
import org.junit.Test
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

public class BigBedFileTest {
    Test fun testWriteReadCompressed() = testWriteRead(true)

    Test fun testWriteReadUncompressed() = testWriteRead(false)

    private fun testWriteRead(compressed: Boolean) {
        val path = Files.createTempFile("example1", ".bb")
        try {
            val bedEntries = BedFile.read(Examples["example1.bed"]).toList()
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes"],
                             path, compressed = compressed)

            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries, bbf.query("chr21", 0, 0).toList())
            }
        } finally {
            Files.deleteIfExists(path)
        }
    }

    Test fun testQueryCompressed() = testQuery(Examples["example1-compressed.bb"])

    Test fun testQueryUncompressed() = testQuery(Examples["example1.bb"])

    private fun testQuery(path: Path) {
        val items = BedFile.read(Examples["example1.bed"]).toList()
        testQuerySmall(path, items)
        testQueryLarge(path, items)
    }

    private fun testQuerySmall(path: Path, items: List<BedEntry>) {
        BigBedFile.read(path).use { bbf ->
            for (i in 0 until 100) {
                testQuery(bbf, items, items[RANDOM.nextInt(items.size())])
            }
        }
    }

    private fun testQueryLarge(path: Path, items: List<BedEntry>) {
        BigBedFile.read(path).use { bbf ->
            for (i in 0 until 10) {
                val a = items[RANDOM.nextInt(items.size())]
                val b = items[RANDOM.nextInt(items.size())]
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

        assertEquals(expected.size(), actual.size());
        assertEquals(expected, actual, message = query.toString())
    }

    Test fun testSummarizeWholeFile() {
        val bbf = BigBedFile.read(Examples["example1.bb"])
        val bedEntries = bbf.query("chr21", 0, 0).toList()
        val (summary) = bbf.summarize(
                bbf.chromosomes.valueCollection().first(),
                0, 0, numBins = 1)
        assertEquals(bedEntries.map { it.end - it.start }.sum().toLong(),
                     summary.count)
        assertEquals(bedEntries.map { it.score }.sum().toDouble(),
                     summary.sum)
    }

    Test fun testSummarizeNoOverlapsTwoBins() {
        var startOffset = RANDOM.nextInt(1000000)
        val bedEntries = (0..(2 pow 16)).asSequence().map {
            val endOffset = startOffset + RANDOM.nextInt(999) + 1
            val score = RANDOM.nextInt(1000)
            val entry = BedEntry("chr1", startOffset, endOffset, ",$score,+")
            startOffset = endOffset + RANDOM.nextInt(100)
            entry
        }.toList().sortBy { it.start }

        testSummarize(bedEntries, numBins = 2)
    }

    Test fun testSummarizeFourBins() {
        val bedEntries = (0..(2 pow 16)).asSequence().map {
            var startOffset = RANDOM.nextInt(1000000)
            val endOffset = startOffset + RANDOM.nextInt(999) + 1
            val score = RANDOM.nextInt(1000)
            val entry = BedEntry("chr1", startOffset, endOffset, ",$score,+")
            entry
        }.toList().sortBy { it.start }

        testSummarize(bedEntries, numBins = 4)
    }

    Test fun testSummarizeManyBins() {
        val bedEntries = (0..(2 pow 16)).asSequence().map {
            var startOffset = RANDOM.nextInt(1000000)
            val endOffset = startOffset + RANDOM.nextInt(999) + 1
            val score = RANDOM.nextInt(1000)
            val entry = BedEntry("chr1", startOffset, endOffset, ",$score,+")
            entry
        }.toList().sortBy { it.start }

        testSummarize(bedEntries, numBins = 1000)
    }

    private fun testSummarize(bedEntries: List<BedEntry>, numBins: Int) {
        val name = bedEntries.map { it.chrom }.first()
        val path = Files.createTempFile("example", ".bb")
        try {
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes"], path)
            BigBedFile.read(path).use { bbf ->
                assertEquals(bedEntries.size(), bbf.query(name, 0, 0).count())

                val summaries = bbf.summarize(name, 0, 0, numBins)
                assertEquals(bedEntries.map { it.end - it.start }.sum().toLong(),
                             summaries.map { it.count }.sum())
                assertTrue(Precision.equals(
                        bedEntries.map { it.score }.sum().toDouble(),
                        summaries.map { it.sum }.sum(), 0.1))
            }
        } finally {
            Files.deleteIfExists(path)
        }
    }

    companion object {
        private val RANDOM = Random()
    }
}
