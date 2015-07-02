package org.jbb.big

import com.google.common.collect.Sets
import com.google.common.math.IntMath
import org.junit.Test
import java.nio.file.Files
import java.util.Random
import java.util.function.Consumer
import java.util.stream.Collectors
import java.util.stream.IntStream
import kotlin.test.*

public class BPlusTreeTest {
    Test fun testReadHeader() {
        BigBedFile.read(Examples.get("example1.bb")).use { bf ->
            val bpt = bf.bPlusTree
            assertEquals(1, bpt.header.blockSize)
            assertEquals(5, bpt.header.keySize)
            assertEquals(8, bpt.header.valSize)
            assertEquals(1, bpt.header.itemCount)
            assertEquals(216L, bpt.header.rootOffset)
        }
    }
    
    Test fun testFind() {
        BigBedFile.read(Examples.get("example1.bb")).use { bf ->
            var leaf = bf.bPlusTree.find(bf.input, "chr1")
            assertNull(leaf)

            leaf = bf.bPlusTree.find(bf.input, "chr21")
            assertNotNull(leaf)
            assertEquals(0, leaf!!.id)
            assertEquals(48129895, leaf.size)
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
                assertNotNull(bpt.find(input, key))
            }

            assertNull(bpt.find(input, "chrV"))
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

    private fun getSequentialItems(itemCount: Int): List<BPlusLeaf> {
        return IntStream.rangeClosed(1, itemCount)
                .mapToObj { i -> BPlusLeaf("chr" + i, i - 1, i * 100) }
                .collect(Collectors.toList())
    }

    Test fun testWriteReadRandom() {
        for (i in 0 until 10) {
            val blockSize = RANDOM.nextInt(64) + 2
            testWriteRead(blockSize, getRandomItems(RANDOM.nextInt(512) + 1))
        }
    }

    private fun getRandomItems(itemCount: Int): List<BPlusLeaf> {
        val names = RANDOM.ints(itemCount.toLong()).distinct().toArray()
        return IntStream.range(0, names.size()).mapToObj { i ->
            val size = Math.abs(RANDOM.nextInt(2 pow 16)) + 1
            BPlusLeaf("chr" + names[i], i, size)
        }.collect(Collectors.toList())
    }

    Test fun testWriteReadRealChromosomes() {
        testWriteRead(3, getExampleItems("f1.chrom.sizes"))
        testWriteRead(4, getExampleItems("f2.chrom.sizes"))
    }

    private fun getExampleItems(example: String): List<BPlusLeaf> {
        val lines = Files.readAllLines(Examples.get(example))
        return IntStream.range(0, lines.size()).mapToObj { i ->
            val chunks = lines[i].split('\t', limit = 2)
            BPlusLeaf(chunks[0], i, chunks[1].toInt())
        }.collect(Collectors.toList())
    }

    private fun testWriteRead(blockSize: Int, items: List<BPlusLeaf>) {
        val path = Files.createTempFile("bpt", ".bb")
        try {
            SeekableDataOutput.of(path).use { output ->
                BPlusTree.write(output, items, blockSize)
            }

            SeekableDataInput.of(path).use { input ->
                val bpt = BPlusTree.read(input, 0)
                for (item in items) {
                    val res = bpt.find(input, item.key)
                    assertNotNull(res)
                    assertEquals(item, res)
                }

                assertEquals(items.toSet(), bpt.traverse(input).toSet())
            }
        } finally {
            Files.delete(path)
        }
    }

    companion object {
        private val RANDOM = Random()
    }
}
