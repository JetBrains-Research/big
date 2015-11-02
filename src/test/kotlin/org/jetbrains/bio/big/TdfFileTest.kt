package org.jetbrains.bio.big

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TdfFileTest {
    @Test fun testHeader() {
        TdfFile.read(Examples["example.tdf"]).use { tdf ->
            assertEquals(4, tdf.header.version)
            assertEquals(listOf("mean"), tdf.windowFunctions.map { it.id })
            assertEquals(TrackType("OTHER"), tdf.trackType)
            assertTrue(tdf.trackLine.isEmpty())
            assertEquals(listOf("one", "two", "three"), tdf.trackNames)
            assertTrue(tdf.build.endsWith("hg18"))
            assertTrue(tdf.compressed)
        }
    }
}
