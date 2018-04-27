package org.jetbrains.bio.big

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.MoreExecutors
import org.jetbrains.bio.*
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.stream.Collectors
import java.util.stream.IntStream
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@RunWith(Parameterized::class)
class BigFileTest(
        private val bfProvider: NamedRomBufferFactoryProvider,
        private val prefetch: Int
) {

    @Test
    fun determineFileTypeBigBed() {
        val src = Examples["example1.bb"].toString()
        val type = BigFile.determineFileType(src) { path, byteOrder ->
            bfProvider(path, byteOrder)
        }
        assertEquals(BigFile.Type.BIGBED, type)
    }

    @Test
    fun determineFileTypeBigWig() {
        val src = Examples["example2.bw"].toString()
        val type = BigFile.determineFileType(src) { path, byteOrder ->
            bfProvider(path, byteOrder)
        }
        assertEquals(BigFile.Type.BIGWIG, type)
    }

    @Test
    fun determineFileTypeUnknown() {
        val src = Examples["example.tdf"].toString()
        val type = BigFile.determineFileType(src) { path, byteOrder ->
            bfProvider(path, byteOrder)
        }
        assertNull(type)
    }

    @Test
    fun testReadHeader() {
        BigFile.read(Examples["example1.bb"], bfProvider, prefetch).use { bf ->
            val header = bf.header
            assertEquals(1, header.version)
            assertEquals(5, header.zoomLevelCount)
            assertEquals(3, header.fieldCount)
            assertEquals(3, header.definedFieldCount)
            assertEquals(0, header.uncompressBufSize)
        }
    }

    @Test
    fun testZoomPartitioning() {
        // In theory we can use either WIG or BED, but WIG is just simpler.
        withTempFile("example2", ".bw") { path ->
            BigWigFile.read(Examples["example2.bw"], bfProvider, prefetch).use { bwf ->
                bwf.buffFactory.create().use { input ->
                    val (name, _/* chromIx */, size) =
                            bwf.bPlusTree.traverse(input).first()
                    BigWigFile.write(bwf.query(name).take(32), listOf(name to size), path)
                }
            }

            BigFile.read(path, bfProvider, prefetch).use { bwf ->
                bwf.buffFactory.create().use { input ->
                    val (_/* name */, chromIx, size) =
                            bwf.bPlusTree.traverse(input).first()
                    val query = Interval(chromIx, 0, size)
                    for ((reduction, _/* dataOffset */, indexOffset) in bwf.zoomLevels) {
                        if (reduction == 0) {
                            break
                        }

                        val zRTree = RTreeIndex.read(input, indexOffset)
                        val blocks = zRTree.findOverlappingBlocks(
                                input, query, bwf.header.uncompressBufSize, null
                        ).toList()
                        for (i in blocks.indices) {
                            for (j in i + 1 until blocks.size) {
                                assertFalse(blocks[i].interval intersects blocks[j].interval)
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testDecompressBlockCaching() {
        BigWigFile.read(Examples["example2.bw"], bfProvider, prefetch).use { bwf ->
            bwf.buffFactory.create().use { input ->
                var cachedValue: Pair<BigFile.RomBufferState, RomBuffer?>?

                cachedValue = BigFile.lastCachedBlockInfoValue()
                assertEquals(cachedValue.first, BigFile.RomBufferState(null, 0, 0, ""))
                assertEquals(cachedValue.second, null)

                var decompressedInput: RomBuffer
                decompressedInput = bwf.decompressAndCacheBlock(input, "chr21", 401, 2281)
                check(decompressedInput is BBRomBuffer) // supposed to by byte array based buffer

                assertEquals(0, decompressedInput.position)
                decompressedInput.readByte()
                assertEquals(1, decompressedInput.position)

                cachedValue = BigFile.lastCachedBlockInfoValue()
                assertEquals(cachedValue.first, BigFile.RomBufferState(bwf.buffFactory, 401, 2281, "chr21"))
                assertNotNull(cachedValue.second)
                assertEquals(0, cachedValue.second!!.position)

                // read cached value & change input
                decompressedInput = bwf.decompressAndCacheBlock(input, "chr21", 401, 2281)
                assertEquals(0, decompressedInput.position)
                decompressedInput.readByte()
                assertEquals(1, decompressedInput.position)

                // ensure cache pos isn't affected:
                //
                // get from cache:
                decompressedInput = bwf.decompressAndCacheBlock(input, "chr21", 401, 2281)
                assertEquals(0, decompressedInput.position)
                // check cache:
                cachedValue = BigFile.lastCachedBlockInfoValue()
                assertEquals(cachedValue.first, BigFile.RomBufferState(bwf.buffFactory, 401, 2281, "chr21"))
                assertNotNull(cachedValue.second)
                assertEquals(0, cachedValue.second!!.position)

                // read next:
                decompressedInput = bwf.decompressAndCacheBlock(input, "chr21", 2682, 2282)
                assertEquals(0, decompressedInput.position)
                cachedValue = BigFile.lastCachedBlockInfoValue()
                assertEquals(cachedValue.first, BigFile.RomBufferState(bwf.buffFactory, 2682, 2282, "chr21"))
                assertNotNull(cachedValue.second)
            }
        }

        val cachedValue = BigFile.lastCachedBlockInfoValue()
        assertEquals(cachedValue.first, BigFile.RomBufferState(null, 0, 0, ""))
        assertEquals(cachedValue.second, null)

    }

    companion object {
        @Parameterized.Parameters(name = "{0}")
        @JvmStatic
        fun data() = romFactoryProviderAndPrefetchParams()

        fun doTestConcurrentChrAccess(
                fileName: String,
                expected: Array<Pair<String, Int>>,
                factoryProvider: NamedRomBufferFactoryProvider,
                prefetch: Int,
                singleThreadMode: Boolean = false
        ) {
            BigFile.read(Examples[fileName], factoryProvider, prefetch).use { bf ->
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

        fun doTestConcurrentDataAccess(
                fileName: String, expected: Array<Pair<Int, Int>>,
                bfProvider: NamedRomBufferFactoryProvider,
                prefetch: Int,
                singleThreadMode: Boolean
        ) {
            BigFile.read(Examples[fileName], bfProvider, prefetch).use { bf ->
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

@RunWith(Parameterized::class)
class BigFileConcurrencyTest(
        private val bfProvider: NamedRomBufferFactoryProvider,
        private val prefetch: Int
) {

    @Test
    fun testConcurrentQuery() {
        BigFile.read(Examples["example2.bw"], bfProvider, prefetch).use { bf ->
            bf.buffFactory.create().use { input ->

                val (name, _/* chromIx */, _/* size */) =
                        bf.bPlusTree.traverse(input).first()

                val executor = MoreExecutors.listeningDecorator(
                        Executors.newFixedThreadPool(8))
                val latch = CountDownLatch(8)
                val futures = (0..7).map {
                    executor.submit {
                        latch.countDown()
                        assertEquals(6857, bf.query(name).count())
                        latch.await()
                    }
                }

                for (future in Futures.inCompletionOrder(futures)) {
                    future.get()
                }

                executor.shutdownNow()
            }
        }
    }

    companion object {
        @Parameterized.Parameters(name = "{0}:{1}")
        @JvmStatic
        fun data() = threadSafeRomFactoryProvidersAndPrefetchParams()
    }
}