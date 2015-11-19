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

import java.io.IOException
import java.nio.file.Paths
/**
 * A Tiled Data Format (TDF) dumper.
 * See https://www.broadinstitute.org/software/igv/TDF.
*/
object TDFUtils {

    @Throws(IOException::class)
    @JvmStatic fun main(args: Array<String>) {
        // Original URL for TDFReaderTest
        // http://data.broadinstitute.org/igvdata/encode/hg18/broadHistone/SignalK562H3k4me3.tdf
        dumpSummary("/home/oleg/work/big/src/test/resources/SignalK562H3k4me3.tdf", false)
    }
    @Throws(IOException::class)
    fun dumpSummary(ibfFile: String, dumpTiles: Boolean) {
        val reader = TDFReader.read(Paths.get(ibfFile))

        println("Version: " + reader.version)
        println("Window Functions")
        for (wf in reader.windowFunctions) {
            println("\t" + wf.toString())
        }

        println("Tracks")
        val trackNames = reader.trackNames
        for (trackName in trackNames) {
            println(trackName)
        }
        println()

        println("DATASETS")
        for (dsName in reader.getDatasetNames()) {
            println(dsName)
            val ds = reader.getDataset(dsName)

            println("Attributes")
            for (entry in ds.attributes.entries) {
                println("\t" + entry.key + " = " + entry.value)
            }
            println()

            println("Tiles")

            val nTracks = trackNames.size
            val tracksToShow = Math.min(4, nTracks)

            for (i in 0 until ds.nTiles) {
                val tile = reader.readTile(ds, i)
                if (tile != null) {
                    print("  " + i)
                    if (dumpTiles) {
                        val nBins = tile.getSize()
                        val binsToShow = Math.min(4, nBins)
                        for (b in 0 until binsToShow) {
                            print(tile.getStartPosition(b))
                            for (t in 0 until tracksToShow) {
                                val value = tile.getValue(0, b)
                                if (!java.lang.Float.isNaN(value)) {
                                    print("\t" + tile.getValue(t, b))
                                }
                            }
                            println()
                        }
                    }
                }
            }
            println()
            println()
        }

        println("GROUPS")
        for (name in reader.getGroupNames()) {
            println(name)
            val group = reader.getGroup(name)

            println("Attributes")
            for (entry in group.attributes.entries) {
                println("\t" + entry.key + " = " + entry.value)
            }
            println()
        }
    }
}
