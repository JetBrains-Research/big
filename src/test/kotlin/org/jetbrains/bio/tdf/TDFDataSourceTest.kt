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
            assertEquals("[(2119283, 2472496, 0.010)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 0, 2500000, 0).toString())
            assertEquals("[(3532138, 3885351, 0.250)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 2500000, 5000000, 0).toString())
            assertEquals("[(2119283, 2472496, 0.010), (3532138, 3885351, 0.250)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 0, 5000000, 0).toString())
        }
    }

    @Test fun testSummaryScoresZoom6() {
        TDFReader.read(Examples["example.tdf"]).use { tdf ->
            assertEquals("[(2146878, 2152396, 0.010)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 0, 2500000, 6).toString())
            assertEquals("[(3554214, 3559732, 0.250)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 2500000, 5000000, 6).toString())
            assertEquals("[(2146878, 2152396, 0.010), (3554214, 3559732, 0.250)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 0, 5000000, 6).toString())
        }
    }

    @Test fun testSummaryScoresZoom10() {
        TDFReader.read(Examples["example.tdf"]).use { tdf ->
            assertEquals("[(2150459, 2150460, 0.010)]",
                    TDFDataSource(tdf, 0).getSummaryScores("chr1", 0, 2500000, 10).toString())
        }
    }
}
