package org.jetbrains.bio.tdf

import org.jetbrains.bio.Examples
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TdfFileTest {
    @Test fun testHeader() {
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

    @Test fun testGetDataset() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            val dataset = tdf.getDataset("All")
            assertTrue(dataset.attributes.isEmpty())
        }
    }

    @Test fun testGetGroup() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            val group = tdf.getGroup("/")
            assertEquals(tdf.build, group.attributes["genome"])
        }
    }

    @Test fun testGetTile() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            val dataset = tdf.getDataset("All")
            val tile = tdf.getTile(dataset, 0)
            // nothing here atm.
        }
    }

    @Test fun testSummarizeZoom0() {
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

    @Test fun testSummarizeZoom6() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            val summary = tdf.summarize("chr1", 0, 2500000, 6)
            assertEquals("[0.01@[2146878; 2152396)]", summary[0].toString())
        }
    }

    @Test fun testSummarizeZoom10() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            val summary = tdf.summarize("chr1", 0, 2500000, 10)
            assertEquals("[0.01@[2150459; 2150460)]", summary[0].toString())
        }
    }
}
