package org.jetbrains.bio.big

import com.google.common.collect.ComparisonChain
import org.jetbrains.bio.bufferedReader
import java.awt.Color
import java.io.IOException
import java.nio.file.Path

class BedFile(private val path: Path) : Iterable<BedEntry> {
    override fun iterator() = path.bufferedReader().lines().map { line ->
        val chunks = line.split('\t', limit = 4)
        BedEntry(chunks[0], chunks[1].toInt(), chunks[2].toInt(),
                 if (chunks.size == 3) "" else chunks[3])
    }.iterator()!!

    companion object {
        @Throws(IOException::class)
        @JvmStatic fun read(path: Path) = BedFile(path)
    }
}

/**
 * A minimal representation of a BED file entry.
 */
data class BedEntry(
        /** Chromosome name, e.g. `"chr9"`. */
        val chrom: String,
        /** 0-based start offset (inclusive). */
        val start: Int,
        /** 0-based end offset (exclusive). */
        val end: Int,
        /** Name of feature. */
        val name: String,
        /** A number from [0, 1000] that controls shading of item. */
        val score: Short,
        /** + or – or . for unknown. */
        val strand: Char,
        /** The starting position at which the feature is drawn thickly. **/
        var thickStart: Int = 0,
        /** The ending position at which the feature is drawn thickly. **/
        var thickEnd: Int = 0,
        /** The colour of entry in the form R,G,B (e.g. 255,0,0). **/
        var itemRgb: Int = 0,

        /** Tab-separated string of additional BED values. */
        val rest: String = "") : Comparable<BedEntry> {

    init {
        require(score in 0..1000) {
            "Unexpected score: $score"
        }

        require(strand == '+' || strand == '-' || strand == '.') {
            "Unexpected strand value: $strand"
        }
    }

    override fun compareTo(other: BedEntry): Int = ComparisonChain.start()
            .compare(chrom, other.chrom)
            .compare(start, other.start)
            .result()

    companion object {
        operator fun invoke(chrom: String, start: Int, end: Int, rest: String = ""): BedEntry {
            val it = rest.split('\t', limit = 7).iterator()

            val name = if (it.hasNext()) it.next() else ""
            val score = if (it.hasNext()) it.next().toShort() else 0
            val strand = if (it.hasNext()) it.next().first() else '.'
            val thickStart = if (it.hasNext()) it.next().toInt() else 0
            val thickEnd = if (it.hasNext()) it.next().toInt() else 0
            val color = if (it.hasNext()) {
                val value = it.next()
                if (value == "0") {
                    0
                } else {
                    val chunks = value.split(',', limit = 3)
                    Color(chunks[0].toInt(), chunks[1].toInt(), chunks[2].toInt()).rgb
                }
            } else {
                0
            }
            val suffix = if (it.hasNext()) it.next() else ""
            return BedEntry(chrom, start, end, name, score, strand, thickStart, thickEnd, color, suffix)
        }
    }
}
