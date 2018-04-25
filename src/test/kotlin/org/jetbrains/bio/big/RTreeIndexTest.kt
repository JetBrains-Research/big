package org.jetbrains.bio.big

import org.jetbrains.bio.*
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.nio.ByteOrder
import java.util.*
import kotlin.LazyThreadSafetyMode.NONE
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@RunWith(Parameterized::class)
class RTreeIndexTest(
        private val bfProvider: NamedRomBufferFactoryProvider,
        private val prefetch: Boolean
) {
    @Test
    fun testReadHeader() {
        BigBedFile.read(Examples["example1.bb"], bfProvider, prefetch).use { bbf ->
            val rti = bbf.rTree
            assertEquals(1024, rti.header.blockSize)
            assertEquals(192771L, rti.header.endDataOffset)
            assertEquals(64, rti.header.itemsPerSlot)
            assertEquals(192819L, rti.header.rootOffset)
            assertNotNull(bbf.rTree.rootNode)
            assertTrue(bbf.rTree.rootNode is RTReeNodeLeaf)
            assertEquals(232, (bbf.rTree.rootNode as RTReeNodeLeaf).leaves.size)

            val items = exampleItems
            assertEquals(items.size.toLong(), rti.header.itemCount)
            assertEquals(0, rti.header.startChromIx)
            assertEquals(0, rti.header.endChromIx)
            assertEquals(items.map { it.start }.min(), rti.header.startBase)
            assertEquals(items.map { it.end }.max(), rti.header.endBase)
        }
    }

    @Test
    fun testWriteEmpty() {
        withTempFile("empty", ".rti") { path ->
            OrderedDataOutput(path).use {
                RTreeIndex.write(it, emptyList())
            }

            bfProvider(path.toString(), ByteOrder.nativeOrder()).use { f ->
                f.create().use { input ->

                    val rti = RTreeIndex.read(input, 0L)
                    if (prefetch) {
                        rti.prefetchBlocksIndex(input, true, false, 0, null)
                    }

                    val query = Interval(0, 100, 200)
                    assertTrue(rti.findOverlappingBlocks(input, query, 0, null).toList().isEmpty())
                }
            }
        }
    }

    @Test
    fun testFindOverlappingBlocksExample() = BigBedFile.read(
            Examples["example1.bb"], bfProvider, prefetch
    ).use { bbf ->
        val rti = bbf.rTree
        val items = exampleItems
        for (i in 0 until 100) {
            val left = RANDOM.nextInt(items.size - 1)
            val right = left + RANDOM.nextInt(items.size - left)
            val query = Interval(0, items[left].start, items[right].end)

            bbf.buffFactory.create().use { input ->
                for (block in rti.findOverlappingBlocks(input, query, bbf.header.uncompressBufSize, null)) {
                    assertTrue(block.interval intersects query)
                }
            }
        }
    }

    @Test
    fun testFindOverlappingLeaves2() = testFindOverlappingLeaves(2)

    @Test
    fun testFindOverlappingLeaves3() = testFindOverlappingLeaves(3)

    @Test
    fun testFindOverlappingLeaves5() = testFindOverlappingLeaves(5)

    private fun testFindOverlappingLeaves(blockSize: Int) {
        withTempFile("random$blockSize", ".rti") { path ->
            val size = 4096
            val step = 1024
            val leaves = ArrayList<RTreeIndexLeaf>()
            var offset = 0
            for (i in 0 until size) {
                val next = offset + RANDOM.nextInt(step) + 1
                val interval = Interval(0, offset, next)
                leaves.add(RTreeIndexLeaf(interval, 0, 0))
                offset = next
            }

            OrderedDataOutput(path).use {
                RTreeIndex.write(it, leaves, blockSize)
            }

            bfProvider(path.toString(), ByteOrder.nativeOrder()).use { f ->
                f.create().use { input ->
                    val rti = RTreeIndex.read(input, 0)
                    if (prefetch) {
                        rti.prefetchBlocksIndex(input, true, false, 0, null)
                    }
                    for (leaf in leaves) {
                        val overlaps = rti.findOverlappingBlocks(
                                input, leaf.interval as ChromosomeInterval, 0, null).toList()
                        assertTrue(overlaps.isNotEmpty())
                        assertEquals(leaf, overlaps.first())
                    }
                }
            }
        }
    }

    companion object {
        private val RANDOM = Random()

        private val exampleItems: List<BedEntry> by lazy(NONE) {
            BedFile.read(Examples["example1.bed"]).use { it.toList() }
        }

        @Parameterized.Parameters(name = "{0}:{1}")
        @JvmStatic
        fun data() = romFactoryProviderAndPrefetchParams()
    }
}

@RunWith(Parameterized::class)
class PrefetchedRTreeIndexTest(private val bfProvider: NamedRomBufferFactoryProvider) {
    @Test
    fun testPrefetch() {
        doTest { input, rti ->
            rti.prefetchBlocksIndex(input, false, false, 0, null)

            assertNotNull(rti.rootNode)

            assertTrue(rti.rootNode!! is RTReeNodeIntermediate)
            val root = rti.rootNode!! as RTReeNodeIntermediate
            assertEquals(2, root.children.size)

            assertTrue(root.children[0].node is RTreeNodeRef)
            assertEquals(124, (root.children[0].node as RTreeNodeRef).dataOffset)
            assertEquals("[0:0; 1:1137275)", root.children[0].interval.toString())
            assertTrue(root.children[1].node is RTreeNodeRef)
            assertEquals(200, (root.children[1].node as RTreeNodeRef).dataOffset)
            assertEquals("1:[1137275; 2102048)", root.children[1].interval.toString())

            val expandOneLevel = root.children[0].resolve(input, rti, 0, null)
            assertTrue(expandOneLevel is RTReeNodeIntermediate)
            assertTrue(((expandOneLevel as RTReeNodeIntermediate).children[2]).node is RTreeNodeRef)
            assertEquals("1:[768454; 1137275)", expandOneLevel.children[2].interval.toString())
        }
    }

    @Test
    fun testPrefetchExpandAll() {
        doTest { input, rti ->
            rti.prefetchBlocksIndex(input, true, false, 0, null)

            assertNotNull(rti.rootNode)

            assertTrue(rti.rootNode!! is RTReeNodeIntermediate)
            val root = rti.rootNode!! as RTReeNodeIntermediate
            assertEquals(2, root.children.size)

            assertEquals("[0:0; 1:1137275)", root.children[0].interval.toString())
            assertTrue(root.children[0].node is RTReeNodeIntermediate)
            assertEquals(3, (root.children[0].node as RTReeNodeIntermediate).children.size)

            assertEquals("1:[1137275; 2102048)", root.children[1].interval.toString())
            assertTrue(root.children[1].node is RTReeNodeIntermediate)
            assertEquals(3, (root.children[1].node as RTReeNodeIntermediate).children.size)

            val childNode = root.children[0].resolve(input, rti, 0, null)
            assertEquals("[0:0; 1:1137275)", root.children[0].interval.toString())
            assertTrue(childNode is RTReeNodeIntermediate)
            assertTrue((childNode as RTReeNodeIntermediate).children[2].node is RTReeNodeIntermediate)
            assertEquals("1:[768454; 1137275)", childNode.children[2].interval.toString())
        }
    }

    @Test
    fun testPrefetchExpandUpToChrs() {
        doTest { input, rti ->
            rti.prefetchBlocksIndex(input, false, true, 0, null)

            assertNotNull(rti.rootNode)

            assertTrue(rti.rootNode!! is RTReeNodeIntermediate)
            val root = rti.rootNode!! as RTReeNodeIntermediate
            assertEquals(2, root.children.size)

            assertTrue(root.children[0].node is RTReeNodeIntermediate)
            assertEquals(3, (root.children[0].node as RTReeNodeIntermediate).children.size)
            assertEquals("[0:0; 1:1137275)", root.children[0].interval.toString())
            assertTrue(root.children[1].node is RTreeNodeRef)
            assertEquals(200, (root.children[1].node as RTreeNodeRef).dataOffset)
            assertEquals("1:[1137275; 2102048)", root.children[1].interval.toString())

            println((root.children[0].node as RTReeNodeIntermediate).children[0].node)
            assertTrue((root.children[0].node as RTReeNodeIntermediate).children[0].node is RTreeNodeRef)
            assertEquals(
                    276,
                    ((root.children[0].node as RTReeNodeIntermediate).children[0].node as RTreeNodeRef).dataOffset)
        }
    }


    private fun doTest(block: (RomBuffer, RTreeIndex) -> Unit) {
        val rnd = Random(100)

        val blockSize = 3
        withTempFile("random$blockSize", ".rti") { path ->
            val size = 4096
            val step = 1024
            val leaves = ArrayList<RTreeIndexLeaf>()
            var offset = 0
            for (i in 0 until size) {
                val next = offset + rnd.nextInt(step) + 1
                val interval = Interval(if (i < 1000) 0 else 1, offset, next)
                leaves.add(RTreeIndexLeaf(interval, 0, 0))
                offset = next
            }

            OrderedDataOutput(path).use {
                RTreeIndex.write(it, leaves, blockSize)
            }

            bfProvider(path.toString(), ByteOrder.nativeOrder()).use { f ->
                f.create().use { input ->
                    val rti = RTreeIndex.read(input, 0)
                    block(input, rti)
                }
            }
        }
    }

    companion object {
        @Parameterized.Parameters(name = "{0}")
        @JvmStatic
        fun data() = romFactoryProviderParams()
    }
}