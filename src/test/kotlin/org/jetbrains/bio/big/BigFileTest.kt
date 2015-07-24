package org.jetbrains.bio.big

import org.junit.Test
import kotlin.test.assertEquals

public class BigFileTest {
    Test fun testReadHeader() {
        BigBedFile.read(Examples.get("example1.bb")).use { bbf ->
            val header = bbf.header
            assertEquals(1, header.version)
            assertEquals(5, header.zoomLevelCount)
            assertEquals(3, header.fieldCount)
            assertEquals(3, header.definedFieldCount)
            assertEquals(0, header.uncompressBufSize)
        }
    }
}