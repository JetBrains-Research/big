package org.jetbrains.bio.tdf

import htsjdk.samtools.seekablestream.SeekablePathStream
import org.jetbrains.bio.BetterSeekableBufferedStream
import org.jetbrains.bio.EndianAwareDataSeekableStream
import org.jetbrains.bio.EndianSynchronizedBufferFactory
import org.jetbrains.bio.Examples
import org.jetbrains.bio.big.BigFileTest.Companion.doTestConcurrentChrAccess
import org.jetbrains.bio.big.BigFileTest.Companion.doTestConcurrentDataAccess
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TdfFileTest {
    @Test
    fun testHeader() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            assertEquals(4, tdf.version)
            assertEquals(listOf(WindowFunction.MEAN), tdf.windowFunctions)
            assertEquals(TrackType("OTHER"), tdf.trackType)
            assertTrue(tdf.trackLine.isEmpty())
            assertEquals(listOf("one", "two", "three"), tdf.trackNames)
            assertTrue(tdf.build.endsWith("hg18"))
            assertTrue(tdf.compressed)
        }
    }

    @Test
    fun testSource() {
        Examples["example.tdf"].let { src ->
            TdfFile.read(src).use { tdf ->
                assertEquals(src.toString(), tdf.source)
            }
        }
    }

    @Test
    fun testGetDataset() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            val dataset = tdf.getDataset("All")
            assertTrue(dataset.attributes.isEmpty())
        }
    }

    @Test
    fun testGetGroup() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            val group = tdf.getGroup("/")
            assertEquals(tdf.build, group.attributes["genome"])
        }
    }

    @Test
    fun testGetTile() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            val dataset = tdf.getDataset("All")
            tdf.getTile(dataset, 0) //tile
            // nothing here atm.
        }
    }

    @Test
    fun testSummarizeZoom0() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            assertEquals("[]", tdf.summarize("chr1", 0, 100, 0)[0].toString())
            assertEquals("[0.01@[2119283; 2472496)]",
                    tdf.summarize("chr1", 0, 2500000, 0)[0].toString())
            assertEquals("[0.25@[3532138; 3885351)]",
                    tdf.summarize("chr1", 2500000, 5000000, 0)[0].toString())
            assertEquals("[0.01@[2119283; 2472496), 0.25@[3532138; 3885351)]",
                    tdf.summarize("chr1", 0, 5000000, 0)[0].toString())
        }
    }

    @Test
    fun testSummarizeZoom6() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            val summary = tdf.summarize("chr1", 0, 2500000, 6)
            assertEquals("[0.01@[2146878; 2152396)]", summary[0].toString())
        }
    }

    @Test
    fun testSummarizeZoom10() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            val summary = tdf.summarize("chr1", 0, 2500000, 10)
            assertEquals("[0.01@[2150459; 2150460)]", summary[0].toString())
        }
    }

    @Test
    fun testConcurrentChrAccess() {
        val expected = arrayOf("chr1" to 13346235, "chr2" to 13351196,
                "chr3" to 13366877, "chr4" to 13423010)

        TdfFile.read(Examples["concurrent.tdf"]).use { tdf ->
            val chrs = listOf("chr1", "chr2", "chr3", "chr4")
            doTestConcurrentChrAccess(chrs, expected) { name, start, end ->
                val metric = tdf.summarize(name, start, end, 10)[0].map { it.score }.sum()
                // let's somehow convert this float value to int
                (metric * 1000).toLong()
            }
        }
    }


    @Test
    fun testConcurrentDataAccess() {
        val expected = arrayOf(
                0 to 1354, 1 to 4690, 2 to 5836, 3 to 0, 4 to 0, 5 to 0, 6 to 0, 7 to 0, 8 to 0, 9 to 5248,
                10 to 3292, 11 to 15627, 12 to 14950, 13 to 15678, 14 to 5980, 15 to 9199, 16 to 3825,
                17 to 11859, 18 to 12182, 19 to 10166, 20 to 10711, 21 to 3733, 22 to 4836, 23 to 8653,
                24 to 6421, 25 to 9687, 26 to 5832, 27 to 8124, 28 to 3367, 29 to 5612, 30 to 4228, 31 to 4442,
                32 to 8327, 33 to 8883, 34 to 25844, 35 to 14300, 36 to 13302, 37 to 4958, 38 to 13594, 39 to 11760,
                40 to 15581, 41 to 12563, 42 to 17817, 43 to 18275, 44 to 22107, 45 to 12684, 46 to 37437,
                47 to 1414334, 48 to 2938475, 49 to 2745951, 50 to 2491699, 51 to 27177, 52 to 18358, 53 to 10788,
                54 to 16138, 55 to 22588, 56 to 22185, 57 to 44227, 58 to 18501, 59 to 32523, 60 to 1033477,
                61 to 1827159, 62 to 12983, 63 to 8553, 64 to 10104, 65 to 38580, 66 to 19029, 67 to 21632,
                68 to 25106, 69 to 16061, 70 to 47220, 71 to 29559, 72 to 21844, 73 to 21312, 74 to 17191,
                75 to 19504, 76 to 17788, 77 to 0, 78 to 0, 79 to 0, 80 to 0, 81 to 0, 82 to 0, 83 to 0
                , 84 to 0, 85 to 0, 86 to 0, 87 to 0, 88 to 0, 89 to 0, 90 to 0, 91 to 0, 92 to 0, 93 to 0,
                94 to 0, 95 to 0, 96 to 0, 97 to 0, 98 to 0, 99 to 0)
        TdfFile.read(Examples["concurrent.tdf"]).use { tdf ->
            doTestConcurrentDataAccess("chr4", expected) { name, start, end ->
                val summarize = tdf.summarize(name, start, end, 10)
                val list = summarize[0]
                val dd = list.map { it.score }.sum()
                (dd * 1000).toLong()
            }
        }
    }

    @Test
    fun factoryWillBeClosedIfEmptyFile() {
        val testData = Examples["empty.tdf"]
        val closeFlag = AtomicInteger(0)
        val stream = object : SeekablePathStream(testData) {
            override fun close() {
                closeFlag.getAndIncrement()
                super.close()
            }
        }
        var bf: TdfFile? = null
        try {
            bf = TdfFile.read(
                    testData.toString(),
                    factoryProvider = { _, byteOrder ->
                        EndianSynchronizedBufferFactory(
                                EndianAwareDataSeekableStream(
                                        BetterSeekableBufferedStream(
                                                stream,
                                                BetterSeekableBufferedStream.DEFAULT_BUFFER_SIZE
                                        )).apply {
                                    order = byteOrder
                                })
                    })
        } catch (e: Exception) {
            // Ignore
        }

        val flagValue = closeFlag.get()
        bf?.close()

        assertEquals(flagValue, 1)
    }

}
