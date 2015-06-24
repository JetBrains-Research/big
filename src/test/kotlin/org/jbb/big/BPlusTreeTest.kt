package org.jbb.big

import com.google.common.collect.Sets
import com.google.common.math.IntMath
import org.junit.Test
import java.nio.file.Files
import java.util.Random
import java.util.stream.Collectors
import java.util.stream.IntStream
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

public class BPlusTreeTest {
    Test fun testFind() {
        BigBedFile.parse(Examples.get("example1.bb")).use { bf ->
            var bptNodeLeaf = bf.header.bPlusTree.find(bf.handle, "chr1")
            assertFalse(bptNodeLeaf.isPresent())

            bptNodeLeaf = bf.header.bPlusTree.find(bf.handle, "chr21")
            assertTrue(bptNodeLeaf.isPresent())
            assertEquals(0, bptNodeLeaf.get().id)
            assertEquals(48129895, bptNodeLeaf.get().size)
        }
    }

    Test fun testFindAllEqualSize() {
        val chromosomes = arrayOf("chr01", "chr02", "chr03", "chr04", "chr05",
                                  "chr06", "chr07", "chr08", "chr09", "chr10",
                                  "chr11")

        testFindAllExample("example2.bb", chromosomes)  // blockSize = 3.
        testFindAllExample("example3.bb", chromosomes)  // blockSize = 4.
    }

    Test fun testFindAllDifferentSize() {
        val chromosomes = arrayOf("chr1", "chr10", "chr11", "chr2", "chr3",
                                  "chr4", "chr5", "chr6", "chr7", "chr8",
                                  "chr9")

        testFindAllExample("example4.bb", chromosomes)  // blockSize = 4.
    }

    private fun testFindAllExample(example: String, chromosomes: Array<String>) {
        val offset = 628L  // magic!
        SeekableDataInput.of(Examples.get(example)).use { input ->
            val bpt = BPlusTree.read(input, offset)
            for (key in chromosomes) {
                assertTrue(bpt.find(input, key).isPresent())
            }

            assertFalse(bpt.find(input, "chrV").isPresent())
        }
    }

    Test fun testCountLevels() {
        assertEquals(2, BPlusTree.countLevels(10, 100))
        assertEquals(2, BPlusTree.countLevels(10, 90))
        assertEquals(2, BPlusTree.countLevels(10, 11))
        assertEquals(1, BPlusTree.countLevels(10, 10))
    }

    Test fun testWriteReadSmall() {
        testWriteRead(2, getSequentialItems(16))
        testWriteRead(2, getSequentialItems(7))  // not a power of 2.
    }

    Test fun testWriteReadLarge() {
        testWriteRead(8, getSequentialItems(IntMath.pow(8, 3)))
    }

    private fun getSequentialItems(itemCount: Int): List<BPlusItem> {
        return IntStream.rangeClosed(1, itemCount)
                .mapToObj { i -> BPlusItem("chr" + i, i - 1, i * 100) }
                .collect(Collectors.toList())
    }

    Test fun testWriteReadRandom() {
        for (i in 0..9) {
            val blockSize = RANDOM.nextInt(64) + 1
            testWriteRead(blockSize, getRandomItems(RANDOM.nextInt(1024) + 1))
        }
    }

    private fun getRandomItems(itemCount: Int): List<BPlusItem> {
        val names = RANDOM.ints(itemCount.toLong()).distinct().toArray()
        return IntStream.range(0, names.size()).mapToObj { i ->
            val size = Math.abs(RANDOM.nextInt()) + 1
            BPlusItem("chr" + names[i], i, size)
        }.collect(Collectors.toList())
    }

    Test fun testWriteReadRealChromosomes() {
        testWriteRead(3, getExampleItems("f1.chrom.sizes"))
        testWriteRead(4, getExampleItems("f2.chrom.sizes"))
    }

    private fun getExampleItems(example: String): List<BPlusItem> {
        val lines = Files.readAllLines(Examples.get(example))
        return IntStream.range(0, lines.size()).mapToObj { i ->
            val chunks = lines[i].split('\t', limit = 2)
            BPlusItem(chunks[0], i, chunks[1].toInt())
        }.collect(Collectors.toList())
    }

    private fun testWriteRead(blockSize: Int, items: List<BPlusItem>) {
        val path = Files.createTempFile("bpt", ".bb")
        try {
            SeekableDataOutput.of(path).use { output ->
                BPlusTree.write(output, blockSize, items)
            }

            SeekableDataInput.of(path).use { input ->
                val bpt = BPlusTree.read(input, 0)
                for (item in items) {
                    val res = bpt.find(input, item.key)
                    assertTrue(res.isPresent())
                    assertEquals(item, res.get())
                }

                val actual = Sets.newHashSet<BPlusItem>()
                bpt.traverse(input) { actual.add(it) }
                assertEquals(items.toSet(), actual)
            }
        } finally {
            Files.delete(path)
        }
    }

    companion object {
        private val RANDOM = Random()
    }
}
