package org.jetbrains.bio.big

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TdfFileTest {
    @Test fun testHeader() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            assertEquals(4, tdf.header.version)
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
            assertEquals(dataset.dataType, TdfDataset.Type.FLOAT)
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
}
