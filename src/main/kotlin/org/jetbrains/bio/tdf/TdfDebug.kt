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

/**
 * Outputs data from a [TdfFile] in a human-readable form.
 */
fun TdfFile.debug(includeTiles: Boolean = false) {
    println("Version: $version")
    println("Window Functions")
    for (wf in windowFunctions) {
        println("\t$wf")
    }

    println("Tracks")
    val trackNames = trackNames
    for (trackName in trackNames) {
        println(trackName)
    }
    println()

    println("DATASETS")
    for (dsName in dataSetNames) {
        println(dsName)
        val ds = getDatasetInternal(dsName)

        println("Attributes")
        for ((key, value) in ds.attributes) {
            println("\t" + key + " = " + value)
        }
        println()

        println("Tiles")
        val nTracks = trackNames.size
        val tracksToShow = Math.min(4, nTracks)
        for (i in 0 until ds.tileCount) {
            val tile = getTile(ds, i)
            if (tile != null) {
                print("  " + i)
                if (includeTiles) {
                    val nBins = tile.size
                    val binsToShow = Math.min(4, nBins)
                    for (b in 0 until binsToShow) {
                        print(tile.getStartOffset(b))
                        for (t in 0 until tracksToShow) {
                            val value = tile.getValue(0, b)
                            if (!value.isNaN()) {
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
    for (name in groupNames) {
        println(name)
        val group = getGroup(name)

        println("Attributes")
        for ((key, value) in group.attributes.entries) {
            println("\t$key = $value")
        }
        println()
    }
}
