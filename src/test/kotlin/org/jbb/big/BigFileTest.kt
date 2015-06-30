package org.jbb.big

import org.junit.Test
import kotlin.test.assertEquals

public class BigFileTest {
    Test fun testReadHeader() {
        BigBedFile.read(Examples.get("example1.bb")).use { bbf ->
            val header = bbf.header
            assertEquals(1.toShort(), header.version)
            assertEquals(5.toShort(), header.zoomLevelCount)
            assertEquals(3.toShort(), header.fieldCount)
            assertEquals(3.toShort(), header.definedFieldCount)
            assertEquals(0, header.uncompressBufSize)
        }
    }
}