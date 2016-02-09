package org.jetbrains.bio.big

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.MoreExecutors
import org.jetbrains.bio.Examples
import org.jetbrains.bio.withTempFile
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
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
            val (name, _chromIx, size) =
                    bwf.bPlusTree.traverse(bwf.input).first()

            val executor = MoreExecutors.listeningDecorator(
                    Executors.newFixedThreadPool(8))
            val latch = CountDownLatch(8)
            val futures = (0..7).map {
                executor.submit {
                    latch.countDown()
                    assertEquals(6857, bwf.duplicate().query(name).count())
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
                val (name, _chromIx, size) =
                        bwf.bPlusTree.traverse(bwf.input).first()
                BigWigFile.write(bwf.query(name).take(32).toList(), listOf(name to size), path)
            }

            BigWigFile.read(path).use { bwf ->
                val (_name, chromIx, size) =
                        bwf.bPlusTree.traverse(bwf.input).first()
                val query = Interval(chromIx, 0, size)
                for ((reduction, _dataOffset, indexOffset) in bwf.zoomLevels) {
                    if (reduction == 0) {
                        break
                    }

                    val zRTree = RTreeIndex.read(bwf.input, indexOffset)
                    val blocks = zRTree.findOverlappingBlocks(bwf.input, query).toList()
                    for (i in blocks.indices) {
                        for (j in i + 1..blocks.size - 1) {
                            assertFalse(blocks[i].interval intersects blocks[j].interval)
                        }
                    }
                }
            }
        }
    }
}