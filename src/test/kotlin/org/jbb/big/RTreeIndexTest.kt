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
            assertEquals(192771L, rti.header.endDataOffset)
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
                val query = Interval.of(0, items[left].start, items[right].end)

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
        BedFile.read(Examples.get("example1.bed")).toList()
    }

    companion object {
        private val RANDOM = Random()
    }
}

// TODO: merge into [RTreeIndexTest].
public class RTreeIndexWriterTest {
    Test fun testWriteBlocks() {
        val chromSizesPath = Examples.get("f2.chrom.sizes")
        val bedPath = Examples.get("bedExample01.txt")
        val indexPath = Files.createTempFile("rti", ".bb")

        val bedSummary = BedSummary.of(bedPath, chromSizesPath)
        val usageList = bedSummary.toList()

        try {
            SeekableDataOutput.of(indexPath).use { output ->
                val bounds = RTreeIndex.writeBlocks(output, bedPath, itemsPerSlot = 3)
                assertEquals(13, bounds.size())
                assertEquals(Interval.of(0, 9434178, 9434610) to 0L, bounds.first())
                assertEquals(Interval.of(10, 13057621, 13058276) to 286L, bounds.last())
            }
        } finally {
            Files.delete(indexPath)
        }
    }

    Test fun testWriteRead() {
        val bedPath = Examples.get("bedExample01.txt")
        val indexPath = Files.createTempFile("rti", ".bb")

        var offset = SeekableDataOutput.of(indexPath).use { output ->
            RTreeIndex.write(output, bedPath, blockSize = 4, itemsPerSlot = 3)
        }

        SeekableDataInput.of(indexPath).use { input ->
            val rti = RTreeIndex.read(input, offset)

            assertEquals(rti.header.blockSize, 4)
            assertEquals(rti.header.itemCount, 13L)
            assertEquals(rti.header.startChromIx, 0)
            assertEquals(rti.header.startBase, 9434178)
            assertEquals(rti.header.endChromIx, 10)
            assertEquals(rti.header.endBase, 13058276)
            assertEquals(rti.header.endDataOffset, 299L)
            assertEquals(rti.header.itemsPerSlot, 3)
            assertEquals(rti.header.rootOffset, 347L)

            val dummy = Interval.of(0, 0, 0)
            checkQuery(rti, input, Interval.of(0, 9434178, 9434611),
                       listOf(RTreeIndexLeaf(dummy, 0, 39), RTreeIndexLeaf(dummy, 39, 13)))

            checkQuery(rti, input, Interval.of(0, 9508110, 9516987), listOf())

            checkQuery(rti, input, Interval.of(1, 9508110, 9516987),
                       listOf(RTreeIndexLeaf(dummy, 52, 26)))

            checkQuery(rti, input, Interval.of(2, 9907597, 10148590),
                       listOf(RTreeIndexLeaf(dummy, 78, 39)))

            checkQuery(rti, input, Interval.of(2, 9908258, 10148590),
                       listOf(RTreeIndexLeaf(dummy, 78, 39)))

            checkQuery(rti, input, Interval.of(10, 13057621, 13058276),
                       listOf(RTreeIndexLeaf(dummy, 286, 13)))
        }
    }

    private fun checkQuery(rti: RTreeIndex, reader: SeekableDataInput,
                           query: ChromosomeInterval,
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