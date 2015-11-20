/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2007-2015 Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.jetbrains.bio.tdf

import org.apache.log4j.Logger
import org.jetbrains.bio.tdf.ScoredInterval

/**
 * Entry point for TDF data manipulation.
 * [getSummaryScores] is thread safe, no additional synchronization required.
 * See https://www.broadinstitute.org/software/igv/TDF.
 */
class TDFDataSource(var reader: TDFReader, val trackNumber: Int) {
    companion object {
        val LOG = Logger.getLogger(TDFDataSource::class.java)
    }

    private var maxPrecomputedZoom = 6

    init {
        val rootGroup = reader.getGroup("/")
        try {
            maxPrecomputedZoom = Integer.parseInt(rootGroup["maxZoom"])
        } catch (e: Exception) {
            LOG.error("Error reading attribute 'maxZoom'", e)
        }
   }

    fun getSummaryScores(chr: String, startLocation: Int, endLocation: Int, zoom: Int): List<ScoredInterval> {
        val tiles = if (zoom <= maxPrecomputedZoom) {
            val dataset = reader.getDatasetZoom(chr, zoom, WindowFunction.mean)
            reader.getTiles(dataset, startLocation, endLocation)
        } else {
            // TODO we can do smarter here, taking into account desired bin size, configured by zoom
            // By definition there are 2^z tiles per chromosome, and 700 bins per tile, where z is the zoom level.
            reader.getTiles(reader.getDataset("/$chr/raw"), startLocation, endLocation)
        }
        return tiles.flatMap { t ->
            (0 until t.getSize()).map {
                val value = t.getValue(trackNumber, it)
                if (value.isNaN()) {
                    return@map null
                }
                val start = t.getStartPosition(it)
                val end = t.getEndPosition(it)
                if (startLocation <= start && end < endLocation) {
                    ScoredInterval(start, end, value)
                } else
                    null
            }
        }.filterNotNull()
    }
}

