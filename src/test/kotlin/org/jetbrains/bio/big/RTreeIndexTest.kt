package org.jetbrains.bio.big

import org.junit.Test
import java.util.Random
import kotlin.properties.Delegates
import kotlin.test.assertEquals
import kotlin.test.assertTrue

public class RTreeIndexTest {
    Test fun testReadHeader() {
        exampleFile.use { bbf ->
            val rti = bbf.rTree
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
            val rti = bbf.rTree
            val items = exampleItems
            for (i in 0 until 100) {
                val left = RANDOM.nextInt(items.size() - 1)
                val right = left + RANDOM.nextInt(items.size() - left)
                val query = Interval(0, items[left].start, items[right].end)

                for (block in rti.findOverlappingBlocks(bbf.input, query)) {
                    assertTrue(block.interval intersects query)
                }
            }
        }
    }

    private val exampleFile: BigBedFile get() {
        return BigBedFile.read(Examples["example1.bb"])
    }

    private val exampleItems: List<BedEntry> by Delegates.lazy {
        BedFile.read(Examples["example1.bed"]).toList()
    }

    companion object {
        private val RANDOM = Random()
    }
}