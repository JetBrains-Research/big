package org.jetbrains.bio.big

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.MoreExecutors
import org.jetbrains.bio.Examples
import org.jetbrains.bio.MMBRomBuffer
import org.jetbrains.bio.withTempFile
import org.junit.Assert
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.stream.Collectors
import java.util.stream.IntStream
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class BigFileTest {
    @Test fun testReadHeader() {
        BigBedFile.read(Examples["example1.bb"]).use { bbf ->
            val header = bbf.header
            assertEquals(1, header.version)
            assertEquals(5, header.zoomLevelCount)
            assertEquals(3, header.fieldCount)
            assertEquals(3, header.definedFieldCount)
            assertEquals(0, header.uncompressBufSize)
        }
    }

    @Test fun testConcurrentQuery() {
        BigWigFile.read(Examples["example2.bw"]).use { bwf ->
            val input = MMBRomBuffer(bwf.memBuff)

            val (name, _/* chromIx */, _/* size */) =
                    bwf.bPlusTree.traverse(input).first()

            val executor = MoreExecutors.listeningDecorator(
                    Executors.newFixedThreadPool(8))
            val latch = CountDownLatch(8)
            val futures = (0..7).map {
                executor.submit {
                    latch.countDown()
                    assertEquals(6857, bwf.query(name).count())
                    latch.await()
                }
            }

            for (future in Futures.inCompletionOrder(futures)) {
                future.get()
            }

            executor.shutdownNow()
        }
    }

    @Test fun testZoomPartitioning() {
        // In theory we can use either WIG or BED, but WIG is just simpler.
        withTempFile("example2", ".bw") { path ->
            BigWigFile.read(Examples["example2.bw"]).use { bwf ->
                val input = MMBRomBuffer(bwf.memBuff)
                val (name, _/* chromIx */, size) =
                        bwf.bPlusTree.traverse(input).first()
                BigWigFile.write(bwf.query(name).take(32).toList(), listOf(name to size), path)
            }

            BigWigFile.read(path).use { bwf ->
                val input = MMBRomBuffer(bwf.memBuff)
                val (_/* name */, chromIx, size) =
                        bwf.bPlusTree.traverse(input).first()
                val query = Interval(chromIx, 0, size)
                for ((reduction, _/* dataOffset */, indexOffset) in bwf.zoomLevels) {
                    if (reduction == 0) {
                        break
                    }

                    val zRTree = RTreeIndex.read(input, indexOffset)
                    val blocks = zRTree.findOverlappingBlocks(input, query).toList()
                    for (i in blocks.indices) {
                        for (j in i + 1 until blocks.size) {
                            assertFalse(blocks[i].interval intersects blocks[j].interval)
                        }
                    }
                }
            }
        }
    }

    companion object {
        fun doTestConcurrentChrAccess(fileName: String,
                                      expected: Array<Pair<String, Int>>,
                                      singleThreadMode: Boolean = false) {
            BigFile.read(Examples[fileName]).use { bf ->
                val chrs = bf.chromosomes.valueCollection().toList()
                doTestConcurrentChrAccess(chrs, expected, singleThreadMode) { name, start, end ->
                    bf.summarize(name, start, end, numBins = 10).map { it.count }.sum()
                }
            }

        }

        fun doTestConcurrentChrAccess(chrs: List<String>,
                                      expected: Array<Pair<String, Int>>,
                                      singleThreadMode: Boolean = false,
                                      summarizeFun: (String, Int, Int) -> Long) {

            val size = 50000000
            val nLocuses = 10000 // race condition better happens when we have more locuses
            val locusSize = size / nLocuses

            val res = chrs.let {
                when {
                    singleThreadMode -> it.stream()
                    else -> it.parallelStream()
                }
            }.map { name ->
                val chrIdx = name.replace("chr", "").toInt()
                val chunkStart = 100000000 + chrIdx * 10000000

                val metric = IntStream.range(0, nLocuses).mapToLong { i ->
                    val start = chunkStart + i * locusSize
                    val end = start + locusSize
                    summarizeFun(name, start, end)
                }.sum()
                name to metric
            }.collect(Collectors.toList()).sortedBy { it.first }

            // For debug:
            //println(res.map { (a, b) -> "$a to $b" }.joinToString())

            // If test fails, first try to run it in single thread mode. In multiple threaded
            // mode result my by affected due to race conditions
            Assert.assertArrayEquals(expected.map { it.first to it.second.toLong() }.toTypedArray(),
                                     res.toTypedArray())
        }

        fun doTestConcurrentDataAccess(fileName: String, expected: Array<Pair<Int, Int>>,
                                       singleThreadMode: Boolean = false) {
            BigFile.read(Examples[fileName]).use { bf ->
                val chrName = bf.chromosomes.valueCollection().first()
                doTestConcurrentDataAccess(chrName, expected, singleThreadMode) { name, start, end ->
                    bf.summarize(name, start, end, numBins = 10).map { it.count }.sum()
                }
            }
        }

        fun doTestConcurrentDataAccess(chrName: String, expected: Array<Pair<Int, Int>>,
                                       singleThreadMode: Boolean = false,
                                       summarizeFun: (String, Int, Int) -> Long) {
            val chunksNum = 100
            val nLocuses = 100 // race condition better happens when we have more locuses

            val size = 50000000
            val chunkSize = size / chunksNum

            val res = IntStream.range(0, chunksNum).let {
                when {
                    singleThreadMode -> it
                    else -> it.parallel()
                }
            }.mapToObj { chunkIdx ->
                val chrIdx = chrName.replace("chr", "").toInt()
                val chunkStart = 100000000 + chrIdx * 10000000 + chunkIdx * chunkSize

                val locusSize = chunkSize / nLocuses
                val metric = IntStream.range(0, nLocuses).mapToLong { i ->
                    val start = chunkStart + i * locusSize
                    val end = start + locusSize
                    summarizeFun(chrName, start, end)
                }.sum()
                chunkIdx to metric
            }.collect(Collectors.toList()).sortedBy { it.first }

            // For debug:
            //println(res.map { (a,b) -> "$a to $b" }.joinToString())

            // If test fails, first try to run it in single thread mode. In multiple threaded
            // mode result my by affected due to race conditions
            Assert.assertArrayEquals(expected.map { it.first to it.second.toLong() }.toTypedArray(),
                                     res.toTypedArray())
        }

    }
}