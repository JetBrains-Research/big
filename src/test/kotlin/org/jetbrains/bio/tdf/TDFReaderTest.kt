package org.jetbrains.bio.tdf

import org.jetbrains.bio.big.Examples
import org.jetbrains.bio.tdf.TDFReader
import org.jetbrains.bio.tdf.TrackType
import org.jetbrains.bio.tdf.WindowFunction
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TDFReaderTest {
    @Test fun testHeader() {
        TDFReader.read(Examples["example.tdf"]).use { tdf ->
            assertEquals(4, tdf.version)
            assertEquals(listOf(WindowFunction.mean), tdf.windowFunctions)
            assertEquals(TrackType("OTHER"), tdf.trackType)
            assertTrue(tdf.trackLine.isEmpty())
            assertEquals(listOf("one", "two", "three"), tdf.trackNames)
            assertTrue(tdf.build.endsWith("hg18"))
            assertTrue(tdf.compressed)
        }
    }

    @Test fun testGetDataset() {
        TDFReader.read(Examples["example.tdf"]).use { tdf ->
            val dataset = tdf.getDatasetZoom("All")
            assertTrue(dataset.attributes.isEmpty())
        }
    }

    @Test fun testGetGroup() {
        TDFReader.read(Examples["example.tdf"]).use { tdf ->
            val group = tdf.getGroup("/")
            assertEquals(tdf.build, group.attributes["genome"])
        }
    }

    @Test fun testGetTile() {
        TDFReader.read(Examples["example.tdf"]).use { tdf ->
            val dataset = tdf.getDatasetZoom("All")
            val tile = tdf.readTile(dataset, 0)
            // nothing here atm.
        }
    }
}
