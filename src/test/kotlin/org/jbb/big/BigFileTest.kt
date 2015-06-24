package org.jbb.big

import org.junit.Test
import kotlin.test.assertEquals


public class BigFileTest {
    Test fun testParseHeader() {
        // http://genome.ucsc.edu/goldenpath/help/bigBed.html
        SeekableDataInput.of(Examples.get("example1.bb")).use { s ->
            val header = BigFile.Header.parse(s, BigBedFile.MAGIC)
            assertEquals(1.toShort(), header.version)
            assertEquals(5.toShort(), header.zoomLevels)
            assertEquals(3.toShort(), header.fieldCount)
            assertEquals(3.toShort(), header.definedFieldCount)
            assertEquals(0, header.uncompressBufSize)
        }
    }
}