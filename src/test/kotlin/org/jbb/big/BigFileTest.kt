package org.jbb.big

import org.junit.Test
import kotlin.test.assertEquals

public class BigFileTest {
    Test fun testReadHeader() {
        // http://genome.ucsc.edu/goldenpath/help/bigBed.html
        SeekableDataInput.of(Examples.get("example1.bb")).use { s ->
            val header = BigFile.Header.read(s, BigBedFile.MAGIC)
            assertEquals(1.toShort(), header.version)
            assertEquals(5, header.zoomLevels.size())
            assertEquals(3.toShort(), header.fieldCount)
            assertEquals(3.toShort(), header.definedFieldCount)
            assertEquals(0, header.uncompressBufSize)
        }
    }
}