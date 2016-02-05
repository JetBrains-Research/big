package org.jetbrains.bio.tdf

import org.jetbrains.bio.Examples
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import kotlin.test.assertEquals

class TdfDebugTest {
    @Test fun testDebug() {
        val stream = ByteArrayOutputStream()
        val so = System.out
        System.setOut(PrintStream(stream))
        TdfFile.read(Examples["example.tdf"]).use { it.debug() }
        System.setOut(so)
        assertEquals("""Version: 4
Window Functions
	MEAN
Tracks
one
two
three

DATASETS
/All/z0/mean
Attributes

Tiles
  0

/chr1/z0/mean
Attributes

Tiles
  0

/chr1/z1/mean
Attributes

Tiles
  0

/chr1/z2/mean
Attributes

Tiles
  0

/chr1/z3/mean
Attributes

Tiles
  0

/chr1/z4/mean
Attributes

Tiles
  0

/chr1/z5/mean
Attributes

Tiles
  0

/chr1/z6/mean
Attributes

Tiles
  0

/chr1/z7/mean
Attributes

Tiles
  1

/chr1/raw
Attributes

Tiles
  21  35

GROUPS
/
Attributes
	genome = /home/user/Downloads/IGVTools/genomes/hg18
	Mean = 0.545
	Minimum = 0.0
	Maximum = 1.31
	90th Percentile = 1.31
	userPercentileAutoscaling = true
	maxZoom = 7
	98th Percentile = 1.31
	chromosomes = chr1,
	Median = 0.48
	2nd Percentile = 0.0
	10th Percentile = 0.0

""", String(stream.toByteArray()).replace("\r\n", "\n"))
    }
}
