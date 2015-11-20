package org.jetbrains.bio.big

import org.jetbrains.bio.CountingDataOutput
import org.jetbrains.bio.SeekableDataInput
import org.jetbrains.bio.withTempFile
import org.junit.Test
import java.util.*
import kotlin.LazyThreadSafetyMode.NONE
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RTreeIndexTest {
    @Test fun testReadHeader() {
        exampleFile.use { bbf ->
            val rti = bbf.rTree
            assertEquals(1024, rti.header.blockSize)
            assertEquals(192771L, rti.header.endDataOffset)
            assertEquals(64, rti.header.itemsPerSlot)
            assertEquals(192819L, rti.header.rootOffset)

            val items = exampleItems
            assertEquals(items.size.toLong(), rti.header.itemCount)
            assertEquals(0, rti.header.startChromIx)
            assertEquals(0, rti.header.endChromIx)
            assertEquals(items.map { it.start }.min(), rti.header.startBase)
            assertEquals(items.map { it.end }.max(), rti.header.endBase)
        }
    }

    @Test fun testWriteEmpty() {
        withTempFile("empty", ".rti") { path ->
            CountingDataOutput.of(path).use { output ->
                RTreeIndex.write(output, emptyList())
            }

            SeekableDataInput.of(path).use { input ->
                val rti = RTreeIndex.read(input, 0L)
                val query = Interval(0, 100, 200)
                assertTrue(rti.findOverlappingBlocks(input, query).toList().isEmpty())
            }
        }
    }

    @Test fun testFindOverlappingBlocksExample() = exampleFile.use { bbf ->
        val rti = bbf.rTree
        val items = exampleItems
        for (i in 0 until 100) {
            val left = RANDOM.nextInt(items.size - 1)
            val right = left + RANDOM.nextInt(items.size - left)
            val query = Interval(0, items[left].start, items[right].end)

            for (block in rti.findOverlappingBlocks(bbf.input, query)) {
                assertTrue(block.interval intersects query)
            }
        }
    }

    @Test fun testFindOverlappingLeaves2() = testFindOverlappingLeaves(2)

    @Test fun testFindOverlappingLeaves3() = testFindOverlappingLeaves(3)

    @Test fun testFindOverlappingLeaves5() = testFindOverlappingLeaves(5)

    private fun testFindOverlappingLeaves(blockSize: Int) {
        withTempFile("random$blockSize", ".rti") { path ->
            val size = 4096
            val step = 1024
            val leaves = ArrayList<RTreeIndexLeaf>()
            var offset = 0
            for (i in 0..size - 1) {
                val next = offset + RANDOM.nextInt(step) + 1
                val interval = Interval(0, offset, next)
                leaves.add(RTreeIndexLeaf(interval, 0, 0))
                offset = next
            }

            CountingDataOutput.of(path).use { RTreeIndex.write(it, leaves, blockSize) }
            SeekableDataInput.of(path).use { input ->
                val rti = RTreeIndex.read(input, 0)
                for (leaf in leaves) {
                    val overlaps = rti.findOverlappingBlocks(
                            input, leaf.interval as ChromosomeInterval).toList()
                    assertTrue(overlaps.isNotEmpty())
                    assertEquals(leaf, overlaps.first())
                }
            }
        }
    }

    private val exampleFile: BigBedFile get() {
        return BigBedFile.read(Examples["example1.bb"])
    }

    private val exampleItems: List<BedEntry> by lazy(NONE) {
        BedFile.read(Examples["example1.bed"]).toList()
    }

    companion object {
        private val RANDOM = Random()
    }
}