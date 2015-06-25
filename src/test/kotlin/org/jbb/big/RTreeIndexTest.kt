package org.jbb.big

import com.google.common.collect.ImmutableList
import org.junit.Test
import java.io.IOException
import java.nio.file.Files
import java.util.*
import java.util.stream.Collectors
import kotlin.properties.Delegates
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

public class RTreeIndexTest {
    Test fun testReadHeader() {
        exampleFile.use { bbf ->
            val rti = bbf.header.rTree
            assertEquals(1024, rti.header.blockSize)
            assertEquals(192771L, rti.header.fileSize)
            assertEquals(64, rti.header.itemsPerSlot)
            assertEquals(192819L, rti.header.rootOffset)

            val items = exampleItems
            assertEquals(items.size().toLong(), rti.header.itemCount)
            assertEquals(0, rti.header.startChromIx)
            assertEquals(0, rti.header.endChromIx)
            assertEquals(items.map { it.start }.min(), rti.header.startBase)
            assertEquals(items.map { it.end }.max(), rti.header.endBase)
        }
    }

    Test fun testFindOverlappingBlocks() {
        exampleFile.use { bbf ->
            val rti = bbf.header.rTree
            val items = exampleItems
            for (i in 0..99) {
                val left = RANDOM.nextInt(items.size() - 1)
                val right = left + RANDOM.nextInt(items.size() - left)
                val query = RTreeInterval.of(0, items[left].start, items[right].end)

                rti.findOverlappingBlocks(bbf.handle, query) { block ->
                    assertTrue(block.interval.overlaps(query))
                }
            }
        }
    }

    private val exampleFile: BigBedFile get() {
        return BigBedFile.read(Examples.get("example1.bb"))
    }

    private val exampleItems: List<BedData> by Delegates.lazy {
        Files.lines(Examples.get("example1.bed")).map { line ->
            val chunks = line.split('\t', limit = 3)
            BedData(0, // doesn't matter.
                    chunks[1].toInt(), chunks[2].toInt(), "")
        }.collect(Collectors.toList<BedData>())
    }

    companion object {
        private val RANDOM = Random()
    }
}

// TODO: merge into [RTreeIndexTest].
public class RTreeIndexWriterTest {
    Test fun testWriteRead() {
        val chromSizesPath = Examples.get("f2.chrom.sizes")
        val bedPath = Examples.get("bedExample01.txt")
        val bigBedPath = Files.createTempFile("bpt", ".bb")

        var rTreeHeaderOffset: Long
        SeekableDataOutput.of(bigBedPath).use { output ->
            // задается для B+ tree
            /* Number of items to bundle in r-tree.  1024 is good. */
            val blockSize = 4
            /* Number of items in lowest level of tree.  64 is good. */
            val itemsPerSlot = 3
            // Берется из as данных: bits16 fieldCount = slCount(as->columnList);
            val fieldCount = 3.toShort()
            rTreeHeaderOffset = RTreeIndex.Header.write(
                    output, chromSizesPath, bedPath, blockSize, itemsPerSlot, fieldCount)
        }

        SeekableDataInput.of(bigBedPath).use { input ->
            val rti = RTreeIndex.read(input, rTreeHeaderOffset)

            assertEquals(rti.header.blockSize, 4)
            assertEquals(rti.header.itemCount, 13L)
            assertEquals(rti.header.startChromIx, 0)
            assertEquals(rti.header.startBase, 9434178)
            assertEquals(rti.header.endChromIx, 10)
            assertEquals(rti.header.endBase, 13058276)
            assertEquals(rti.header.fileSize, 299L)
            assertEquals(rti.header.itemsPerSlot, 1)
            assertEquals(rti.header.rootOffset, 347L)

            val dummy = RTreeInterval.of(0, 0, 0)
            checkQuery(rti, input, RTreeInterval.of(0, 9434178, 9434611),
                       listOf(RTreeIndexLeaf(dummy, 0, 39), RTreeIndexLeaf(dummy, 39, 13)))

            checkQuery(rti, input, RTreeInterval.of(0, 9508110, 9516987), listOf())

            checkQuery(rti, input, RTreeInterval.of(1, 9508110, 9516987),
                       listOf(RTreeIndexLeaf(dummy, 52, 26)))

            checkQuery(rti, input, RTreeInterval.of(2, 9907597, 10148590),
                       listOf(RTreeIndexLeaf(dummy, 78, 39)))

            checkQuery(rti, input, RTreeInterval.of(2, 9908258, 10148590),
                       listOf(RTreeIndexLeaf(dummy, 78, 39)))

            checkQuery(rti, input, RTreeInterval.of(10, 13057621, 13058276),
                       listOf(RTreeIndexLeaf(dummy, 286, 13)))
        }
    }

    private fun checkQuery(rti: RTreeIndex, reader: SeekableDataInput,
                           query: RTreeInterval,
                           expected: List<RTreeIndexLeaf>) {
        val actual = ArrayList<RTreeIndexLeaf>()
        rti.findOverlappingBlocks(reader, query) { actual.add(it) }

        assertEquals(expected.size(), actual.size())
        for (i in expected.indices) {
            // We don't compare the intervals, because 'expected' holds dummies.
            assertEquals(expected[i].dataOffset, actual[i].dataOffset)
            assertEquals(expected[i].dataSize, actual[i].dataSize)
        }
    }
}

public class RTreeIntervalTest {
    Test fun testOverlapsSameChromosome() {
        val interval = RTreeInterval.of(1, 100, 200)
        assertOverlaps(interval, interval)
        assertOverlaps(interval, RTreeInterval.of(1, 50, 150))
        assertOverlaps(interval, RTreeInterval.of(1, 50, 250))
        assertOverlaps(interval, RTreeInterval.of(1, 150, 250))
        assertNotOverlaps(interval, RTreeInterval.of(1, 250, 300))
        // This is OK because right end is exclusive.
        assertNotOverlaps(interval, RTreeInterval.of(1, 200, 300))
    }

    Test fun testOverlapsDifferentChromosomes() {
        assertNotOverlaps(RTreeInterval.of(1, 100, 200), RTreeInterval.of(2, 50, 150))
        assertNotOverlaps(RTreeInterval.of(1, 100, 200), RTreeInterval.of(2, 50, 3, 150))

        assertOverlaps(RTreeInterval.of(1, 100, 2, 200), RTreeInterval.of(2, 50, 3, 150))
        assertNotOverlaps(RTreeInterval.of(1, 100, 2, 200), RTreeInterval.of(2, 300, 3, 400))
        assertOverlaps(RTreeInterval.of(1, 100, 2, 200), RTreeInterval.of(2, 50, 3, 250))
        assertOverlaps(RTreeInterval.of(1, 100, 3, 200), RTreeInterval.of(2, 50, 3, 250))
        assertOverlaps(RTreeInterval.of(1, 100, 3, 200), RTreeInterval.of(2, 300, 3, 400))
        assertOverlaps(RTreeInterval.of(1, 100, 3, 200), RTreeInterval.of(2, 50, 3, 100))
    }

    private fun assertOverlaps(interval1: RTreeInterval, interval2: RTreeInterval) {
        assertTrue(interval1.overlaps(interval2), "$interval1 must overlap $interval2")
        assertTrue(interval2.overlaps(interval1), "$interval2 must overlap $interval1")
    }

    private fun assertNotOverlaps(interval1: RTreeInterval, interval2: RTreeInterval) {
        assertFalse(interval1.overlaps(interval2),
                    "$interval1 must not overlap $interval2")
        assertFalse(interval2.overlaps(interval1),
                    "$interval1 must not overlap $interval1")
    }
}

public class RTreeOffsetTest {
    Test fun testCompareToSameChromosome() {
        val offset = RTreeOffset(1, 100)
        assertTrue(offset > RTreeOffset(1, 50))
        assertTrue(RTreeOffset(1, 50) < offset)
        assertTrue(offset == offset)
    }

    Test fun testCompareToDifferentChromosomes() {
        val offset = RTreeOffset(1, 100)
        assertTrue(offset < RTreeOffset(2, 100))
        assertTrue(RTreeOffset(2, 100) > offset)
        assertTrue(offset < RTreeOffset(2, 50))
        assertTrue(RTreeOffset(2, 50) > offset)
    }
}