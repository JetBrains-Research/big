package org.jetbrains.bio.big

import com.google.common.base.MoreObjects
import com.google.common.base.Splitter
import com.google.common.collect.ComparisonChain
import com.google.common.primitives.Ints
import org.jetbrains.bio.bufferedReader
import java.awt.Color
import java.io.IOException
import java.lang.Integer.min
import java.nio.file.Path
import java.util.*

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
        /** Tab-separated string of additional BED values. */
        val rest: String = ""):  Comparable<BedEntry> {


    override fun compareTo(other: BedEntry): Int = ComparisonChain.start()
            .compare(chrom, other.chrom)
            .compare(start, other.start)
            .result()

    /**
     * Parses minimal representation
     * @param fieldsNumber Expected BED format fields number to parse (3..12)
     * @param extraFieldsNumber BED+ format extra fields number to parse, if null parse all extra fields
     */
    fun unpack(fieldsNumber: Byte = 12, extraFieldsNumber: Int? = null): ExtendedBedEntry {

        check(fieldsNumber in 3..12) { "Fields number expected 3..12, but was $fieldsNumber" }
        // This impl parsed only BED9 and adds 9..12 fields to 'rest' string
        val it = when {
            rest.isEmpty() -> emptyArray<String>().iterator()
            else -> rest.split('\t',
                               limit = fieldsNumber.toInt() - 3 + 1).iterator()
        }
        // If line is shorter than suggested fields number do not throw an error
        // it could be ok for default behaviour, when user not sure how much fields
        // do we actually have
        val name = if (fieldsNumber >= 4 && it.hasNext()) it.next() else ""
        val score = if (fieldsNumber >= 5 && it.hasNext()) it.next().toShort() else 0
        val strand = if (fieldsNumber >= 6 && it.hasNext()) it.next().first() else '.'
        val thickStart = if (fieldsNumber >= 7 && it.hasNext()) it.next().toInt() else 0
        val thickEnd = if (fieldsNumber >= 8 && it.hasNext()) it.next().toInt() else 0
        val color = if (fieldsNumber >= 9 && it.hasNext()) {
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
        val blockCount = if (fieldsNumber >= 10 && it.hasNext()) it.next().toInt() else 0
        val blockSizes = if (fieldsNumber >= 11 && it.hasNext()) {
            val value = it.next()
            if (blockCount > 0) value.splitToInts(blockCount) else null
        } else null
        val blockStarts = if (fieldsNumber >= 12 && it.hasNext()) {
            val value = it.next()
            if (blockCount > 0) value.splitToInts(blockCount) else null
        } else null

        val extraFields = if (extraFieldsNumber != 0 && it.hasNext()) {
            val extraStr = it.next()
            if (extraStr.isEmpty()) {
                null
            } else {
                val extraFields = extraStr.split("\t", limit = (extraFieldsNumber ?: -1) + 1)
                if (extraFieldsNumber == null) {
                    extraFields.toTypedArray()
                } else {
                    Array(min(extraFieldsNumber, extraFields.size)) { i -> extraFields[i] }
                }
            }
        } else {
            null
        }
        return ExtendedBedEntry(chrom, start, end, name, score, strand, thickStart, thickEnd, color,
                                blockCount, blockSizes, blockStarts, extraFields)
    }

    private fun String.splitToInts(size: Int): IntArray {
        val chunks = IntArray(size)
        val s = Splitter.on(',').split(this).iterator()
        var ptr = 0
        // actual fields my be less that size, but not vice versa
        check(s.hasNext() == (size > 0))
        while (s.hasNext() && ptr < size) {
            chunks[ptr++] = s.next().toInt()
        }

        return chunks
    }
}

/**
 * An extended representation of a BED file entry.
 */
data class ExtendedBedEntry(
        /** Chromosome name, e.g. `"chr9"`. */
        val chrom: String,
        /** 0-based start offset (inclusive). */
        val start: Int,
        /** 0-based end offset (exclusive). */
        val end: Int,
        /** Name of feature. */
        val name: String = "",
        /** A number from [0, 1000] that controls shading of item. */
        val score: Short = 0,

        /** + or – or . for unknown. */
        val strand: Char = '.',
        /** The starting position at which the feature is drawn thickly. **/
        var thickStart: Int = 0,
        /** The ending position at which the feature is drawn thickly. **/
        var thickEnd: Int = 0,
        /** The colour of entry in the form R,G,B (e.g. 255,0,0). **/
        var itemRgb: Int = 0,

        val blockCount: Int = 0,
        val blockSizes: IntArray? = null,
        val blockStarts: IntArray? = null,

        /** Additional BED values. */
        val extraFields: Array<String>? = null) {

    init {
        require(score in 0..1000) {
            "Unexpected score: $score"
        }

        require(strand == '+' || strand == '-' || strand == '.') {
            "Unexpected strand value: $strand"
        }
    }


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ExtendedBedEntry) return false

        if (chrom != other.chrom) return false
        if (start != other.start) return false
        if (end != other.end) return false
        if (name != other.name) return false
        if (score != other.score) return false
        if (strand != other.strand) return false
        if (thickStart != other.thickStart) return false
        if (thickEnd != other.thickEnd) return false
        if (itemRgb != other.itemRgb) return false
        if (blockCount != other.blockCount) return false
        if (!Arrays.equals(blockSizes, other.blockSizes)) return false
        if (!Arrays.equals(blockStarts, other.blockStarts)) return false
        if (!Arrays.equals(extraFields, other.extraFields)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = chrom.hashCode()
        result = 31 * result + start
        result = 31 * result + end
        result = 31 * result + name.hashCode()
        result = 31 * result + score
        result = 31 * result + strand.hashCode()
        result = 31 * result + thickStart
        result = 31 * result + thickEnd
        result = 31 * result + itemRgb
        result = 31 * result + blockCount
        result = 31 * result + (blockSizes?.let { Arrays.hashCode(it) } ?: 0)
        result = 31 * result + (blockStarts?.let { Arrays.hashCode(it) } ?: 0)
        result = 31 * result + (extraFields?.let { Arrays.hashCode(it) } ?: 0)
        return result
    }

    override fun toString() = MoreObjects.toStringHelper(this)
                .add("chrom", chrom)
                .add("start", start).add("end", end)
                .add("name", name)
                .add("score", score)
                .add("strand", strand)
                .add("thickStart", thickStart).add("thickEnd", thickEnd)
                .add("itemRgb", itemRgb)
                .add("blocks", when {
                    blockCount == 0 || blockSizes == null -> "[]"
                    blockStarts == null -> Arrays.toString(blockSizes)
                    else -> blockStarts.zip(blockSizes)
                }).add("extra", extraFields?.joinToString("\t") ?: "")
            .toString()

    /**
     * Convert to BedEntry
     * @param fieldsNumber BED format fields number to serialize (3..12)
     * @param extraFieldsNumber BED+ format extra fields number to serialize, if null serialize all extra fields
     */
    fun pack(fieldsNumber: Byte = 12, extraFieldsNumber: Int? = null): BedEntry {
        check(fieldsNumber in 3..12) { "Fields number expected 3..12, but was $fieldsNumber" }

        val rest = ArrayList<String>()

        if (fieldsNumber >= 4) {
            rest.add(name)
        }
        if (fieldsNumber >= 5) {
            rest.add("$score")
        }
        if (fieldsNumber >= 6) {
            rest.add("$strand")
        }
        if (fieldsNumber >= 7) {
            rest.add("$thickStart")
        }
        if (fieldsNumber >= 8) {
            rest.add("$thickEnd")
        }
        if (fieldsNumber >= 9) {
            if (itemRgb == 0) {
                rest.add("0")
            } else {
                val c = Color(itemRgb)
                rest.add(Ints.join(",", c.red, c.green, c.blue))
            }
        }
        if (fieldsNumber >= 10) {
            rest.add("$blockCount")
        }
        if (fieldsNumber >= 11) {
            if (blockCount == 0) {
                rest.add(".")
            } else {
                val nBlockSizes = blockSizes?.size ?: 0
                check(nBlockSizes == blockCount) {
                    "$chrom:$start-$end: Expected blocks number $blockCount != actual block sizes $nBlockSizes"
                }
                rest.add(blockSizes!!.joinToString(","))
            }
        }
        if (fieldsNumber >= 12) {
            if (blockCount == 0) {
                rest.add(".")
            } else {
                val nBlockStarts = blockStarts?.size ?: 0
                check(nBlockStarts == blockCount) {
                    "$chrom:$start-$end: Expected blocks number $blockCount != actual block starts $nBlockStarts"
                }
                rest.add(blockStarts!!.joinToString(","))
            }
        }

        if (extraFields != null && extraFields.isNotEmpty()) {
            if (extraFieldsNumber == null) {
                rest.addAll(extraFields)
            } else if (extraFieldsNumber > 0) {
                check(extraFields.size >= extraFieldsNumber) {
                    "$chrom:$start-$end: Expected extra fields $extraFieldsNumber != actual" +
                            " number ${extraFields.size}"
                }
                (0 until extraFieldsNumber).mapTo(rest) { extraFields[it] }
            }
        }

        return BedEntry(chrom, start, end, rest.joinToString("\t"))
    }
}
