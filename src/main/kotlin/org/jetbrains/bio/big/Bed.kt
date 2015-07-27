package org.jetbrains.bio.big

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import kotlin.platform.platformStatic

class BedFile(private val path: Path) : Iterable<BedEntry> {
    override fun iterator(): Iterator<BedEntry> = Files.lines(path).map { line ->
        val chunks = line.split('\t', limit = 4)
        BedEntry(chunks[0], chunks[1].toInt(), chunks[2].toInt(),
                 if (chunks.size() == 3) "" else chunks[3])
    }.iterator()

    companion object {
        throws(IOException::class)
        public platformStatic fun read(path: Path): BedFile = BedFile(path)
    }
}

/**
 * A minimal representation of a BED file entry.
 */
public data class BedEntry(
        /** Chromosome name, e.g. `"chr9"`. */
        public val chrom: String,
        /** 0-based start offset (inclusive). */
        public val start: Int,
        /** 0-based end offset (exclusive). */
        public val end: Int,
        rest: String = "") {

    /** Name of feature. */
    public val name: String
    /** A number from [0, 1000] that controls shading of item. */
    public val score: Short
    /** + or – or . for unknown. */
    public val strand: Char
    /** Comma-separated string of additional BED values. */
    public val rest: String

    init {
        val it = rest.split(',', limit = 4).iterator()
        name = if (it.hasNext()) it.next() else ""
        score = if (it.hasNext()) it.next().toShort() else 0
        require(score >= 0 && score <= 1000)
        strand = if (it.hasNext()) it.next().first() else '.'
        require(strand == '+' || strand == '-' || strand == '.')
        this.rest = if (it.hasNext()) it.next() else ""
    }
}
