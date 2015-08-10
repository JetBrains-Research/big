package org.jetbrains.bio.big

import org.apache.commons.math3.util.Precision
import org.junit.Test
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

public class BigBedFileTest {
    Test fun testWriteReadSmall() {
        val path = Files.createTempFile("small", ".bb")
        try {
            val bedEntries = listOf(BedEntry("chr21", 0, 100))
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"], path)

            BigBedFile.read(path).use { bbf ->
                assertEquals(1, bbf.query("chr21", 0, 0).count())
                assertEquals(bedEntries, bbf.query("chr21", 0, 0).toList())
            }
        } finally {
            Files.deleteIfExists(path)
        }
    }

    Test fun testWriteReadCompressedBE() = testWriteRead(true, ByteOrder.BIG_ENDIAN)

    Test fun testWriteReadUncompressedBE() = testWriteRead(false, ByteOrder.BIG_ENDIAN)

    Test fun testWriteReadCompressedLE() = testWriteRead(true, ByteOrder.LITTLE_ENDIAN)

    Test fun testWriteReadUncompressedLE() = testWriteRead(false, ByteOrder.LITTLE_ENDIAN)

    private fun testWriteRead(compressed: Boolean, order: ByteOrder) {
        val path = Files.createTempFile("example1", ".bb")
        try {
            val bedEntries = BedFile.read(Examples["example1.bed"]).toList()
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"],
                             path, compressed = compressed, order = order)

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

    Test fun testAggregate() {
        assertEquals(emptyList<BedEntry>(), emptySequence<BedEntry>().aggregate())
        assertEquals(listOf(BedEntry("chr1", 0, 100, "", 1, '.')),
                     sequenceOf(BedEntry("chr1", 0, 100)).aggregate())

        // Duplicate intervals.
        assertEquals(listOf(BedEntry("chr1", 0, 100, "", 2, '.')),
                     sequenceOf(BedEntry("chr1", 0, 100),
                                BedEntry("chr1", 0, 100)).aggregate())
        // Consecutive intervals.
        assertEquals(listOf(BedEntry("chr1", 0, 100, "", 2, '.'),
                            BedEntry("chr1", 100, 110, "", 1, '.')),
                     sequenceOf(BedEntry("chr1", 0, 100),
                                BedEntry("chr1", 0, 110)).aggregate())
        // Disjoint intervals.
        assertEquals(listOf(BedEntry("chr1", 0, 100, "", 1, '.'),
                            BedEntry("chr1", 200, 300, "", 1, '.')),
                     sequenceOf(BedEntry("chr1", 0, 100),
                                BedEntry("chr1", 200, 300)).aggregate())
        // Shifted intervals.
        assertEquals(listOf(BedEntry("chr1", 100, 200, "", 1, '.'),
                            BedEntry("chr1", 200, 300, "", 1, '.')),
                     sequenceOf(BedEntry("chr1", 100, 200),
                                BedEntry("chr1", 200, 300)).aggregate())
    }

    Test fun testSummarizeWholeFile() {
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

    Test fun testSummarizeNoOverlapsTwoBins() {
        var startOffset = RANDOM.nextInt(1000000)
        val bedEntries = (0..(2 pow 16)).asSequence().map {
            val endOffset = startOffset + RANDOM.nextInt(999) + 1
            val entry = BedEntry("chr1", startOffset, endOffset)
            startOffset = endOffset + RANDOM.nextInt(100)
            entry
        }.toList().sortBy { it.start }

        testSummarize(bedEntries, numBins = 2)
    }

    Test fun testSummarizeFourBins() {
        val bedEntries = (0..(2 pow 16)).asSequence().map {
            var startOffset = RANDOM.nextInt(1000000)
            val endOffset = startOffset + RANDOM.nextInt(999) + 1
            val entry = BedEntry("chr1", startOffset, endOffset)
            entry
        }.toList().sortBy { it.start }

        testSummarize(bedEntries, numBins = 4)
    }

    Test fun testSummarizeManyBins() {
        val bedEntries = (0..(2 pow 16)).asSequence().map {
            var startOffset = RANDOM.nextInt(1000000)
            val endOffset = startOffset + RANDOM.nextInt(999) + 1
            val entry = BedEntry("chr1", startOffset, endOffset)
            entry
        }.toList().sortBy { it.start }

        testSummarize(bedEntries, numBins = 1000)
    }

    private fun testSummarize(bedEntries: List<BedEntry>, numBins: Int) {
        val name = bedEntries.map { it.chrom }.first()
        val path = Files.createTempFile("example", ".bb")
        try {
            BigBedFile.write(bedEntries, Examples["hg19.chrom.sizes.gz"], path)
            BigBedFile.read(path).use { bbf ->
                val aggregate = bedEntries.asSequence().aggregate()
                val summaries = bbf.summarize(name, 0, 0, numBins)
                assertEquals(aggregate.map { it.end - it.start }.sum().toLong(),
                             summaries.map { it.count }.sum())
                assertTrue(Precision.equals(
                        aggregate.map { it.score }.sum().toDouble(),
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
