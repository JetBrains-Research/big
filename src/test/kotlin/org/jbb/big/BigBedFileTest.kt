package org.jbb.big

import org.junit.Test
import java.nio.file.Files
import java.util.Random
import java.util.stream.Collectors
import kotlin.properties.Delegates
import kotlin.test.assertEquals
import kotlin.test.assertTrue

public class BigBedFileTest {
    Test fun testCompressed() {
        val items = BedFile.read(Examples.get("example1.bed")).toList()
        testQuerySmall(BigBedFile.read(Examples.get("example1-compressed.bb")),
                       items)
        testQueryLarge(BigBedFile.read(Examples.get("example1-compressed.bb")),
                       items)
    }

    Test fun testUncompressed() {
        val items = BedFile.read(Examples.get("example1.bed")).toList()
        testQuerySmall(BigBedFile.read(Examples.get("example1.bb")),
                       items)
        testQueryLarge(BigBedFile.read(Examples.get("example1.bb")),
                       items)
    }

    private fun testQuerySmall(bbf: BigBedFile, items: List<BedData>) {
        bbf.use { bbf ->
            for (i in 0..99) {
                testQuery(bbf, items, items[RANDOM.nextInt(items.size())])
            }
        }
    }

    private fun testQueryLarge(bbf: BigBedFile, items: List<BedData>) {
        bbf.use { bbf ->
            for (i in 0..9) {
                val a = items[RANDOM.nextInt(items.size())]
                val b = items[RANDOM.nextInt(items.size())]
                testQuery(bbf, items, BedData(a.name, Math.min(a.start, b.start),
                                              Math.max(a.end, b.end)))
            }
        }
    }

    private fun testQuery(bbf: BigBedFile, items: List<BedData>, query: BedData) {
        val actual = bbf.query(query.name, query.start, query.end)
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
