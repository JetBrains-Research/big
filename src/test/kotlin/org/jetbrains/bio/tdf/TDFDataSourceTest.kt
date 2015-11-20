package org.jetbrains.bio.tdf

import org.jetbrains.bio.big.Examples
import org.jetbrains.bio.tdf.TDFReader
import org.jetbrains.bio.tdf.TrackType
import org.jetbrains.bio.tdf.WindowFunction
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TDFDataSourceTest {
    @Test fun testSummaryScoresZoom0() {
        TDFReader.read(Examples["example.tdf"]).use { tdf ->
            assertEquals("[]", TDFDataSource(tdf, 0).getSummaryScores("chr1", 0, 100, 0).toString())
            assertEquals("[0.01@[2119283; 2472496)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 0, 2500000, 0).toString())
            assertEquals("[0.25@[3532138; 3885351)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 2500000, 5000000, 0).toString())
            assertEquals("[0.01@[2119283; 2472496), 0.25@[3532138; 3885351)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 0, 5000000, 0).toString())
        }
    }

    @Test fun testSummaryScoresZoom6() {
        TDFReader.read(Examples["example.tdf"]).use { tdf ->
            assertEquals("[0.01@[2146878; 2152396)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 0, 2500000, 6).toString())
        }
    }

    @Test fun testSummaryScoresZoom10() {
        TDFReader.read(Examples["example.tdf"]).use { tdf ->
            assertEquals("[0.01@[2150459; 2150460)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 0, 2500000, 10).toString())
        }
    }
}
