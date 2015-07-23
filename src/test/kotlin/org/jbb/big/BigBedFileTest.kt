package org.jbb.big

import org.junit.Test
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue

public class BigBedFileTest {
    Test fun testWriteQueryCompressed() = testWriteQuery(true)

    Test fun testWriteQueryUncompressed() = testWriteQuery(false)

    private fun testWriteQuery(compressed: Boolean) {
        val path = Files.createTempFile("example1", ".bb")
        try {
            BigBedFile.write(BedFile.read(Examples.get("example1.bed")),
                             Examples.get("hg19.chrom.sizes"),
                             path, compressed = compressed)

            testQuery(path)
        } finally {
            Files.deleteIfExists(path)
        }
    }

    Test fun testQueryCompressed() = testQuery(Examples.get("example1-compressed.bb"))

    Test fun testQueryUncompressed() = testQuery(Examples.get("example1.bb"))

    private fun testQuery(path: Path) {
        val items = BedFile.read(Examples.get("example1.bed")).toList()
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
                testQuery(bbf, items, BedEntry(a.name, Math.min(a.start, b.start),
                                               Math.max(a.end, b.end)))
            }
        }
    }

    private fun testQuery(bbf: BigBedFile, items: List<BedEntry>, query: BedEntry) {
        val actual = bbf.query(query.name, query.start, query.end).toList()
        for (item in actual) {
            assertTrue(item.start >= query.start && item.end <= query.end)
        }

        val expected = items.asSequence()
                .filter { it.start >= query.start && it.end <= query.end }
                .toList()

        assertEquals(expected.size(), actual.size());
        assertEquals(expected, actual, message = query.toString())
    }

    companion object {
        private val RANDOM = Random()
    }
}
