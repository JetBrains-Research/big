package org.jetbrains.bio.big

import com.google.common.base.MoreObjects
import com.google.common.base.Splitter
import com.google.common.collect.ComparisonChain
import com.google.common.primitives.Ints
import org.jetbrains.bio.bufferedReader
import java.awt.Color
import java.io.Closeable
import java.io.IOException
import java.nio.file.Path
import java.util.*

class BedFile(val path: Path) : Iterable<BedEntry>, Closeable {
    private val reader = path.bufferedReader()

    override fun iterator(): Iterator<BedEntry> {
        return reader.lines().map { line ->
            val chunks = line.split('\t', limit = 4)
            BedEntry(chunks[0], chunks[1].toInt(), chunks[2].toInt(),
                if (chunks.size == 3) "" else chunks[3])
        }.iterator()
    }

    override fun close() {
        reader.close()
    }

    companion object {
        @Throws(IOException::class)
        @JvmStatic fun read(path: Path) = BedFile(path)
    }
}

/**
 * A minimal representation of a BED file entry.
 *
 * The BED standard absolutely requires three fields: [chrom], [start] and [end]. The remaining line is
 * stored in [rest]. It might or might not contain other, more optional BED fields, such as name, score or color.
 * Use [unpack] to obtain an [ExtendedBedEntry] where these fields are properly parsed.
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
     * Unpacks a basic bed3 [BedEntry] into a bedN+ [ExtendedBedEntry].
     *
     * Correctly parses the typed BED fields (such as score, color or blockSizes). Extra (non-standard) fields,
     * if any (and if [parseExtraFields] is enabled) are stored in [ExtendedBedEntry.extraFields]
     * property as a [String] array (null if no extra fields or [parseExtraFields] is disabled).
     *
     * @throws BedEntryUnpackException if this entry couldn't be parsed, either because there are too few fields
     * or because the field values don't conform to the standard, e.g. score is not an integer number. The exception
     * contains the BED entry and the index of the offending field.
     *
     * @param fieldsNumber Expected regular BED format fields number to parse (3..12)
     * @param parseExtraFields Whether to parse or discard the BED+ format extra fields.
     * @param delimiter Custom delimiter for malformed data
     * @param omitEmptyStrings Treat several consecutive separators as one
     */
    fun unpack(
            fieldsNumber: Byte = 12,
            parseExtraFields: Boolean = true,
            delimiter: Char = '\t',
            omitEmptyStrings: Boolean = false
    ): ExtendedBedEntry {

        check(fieldsNumber in 3..12) { "Fields number expected in range 3..12, but was $fieldsNumber" }

        val limit = if (parseExtraFields) 0 else fieldsNumber.toInt() - 3 + 1

        val fields = when {
            rest.isEmpty() -> emptyList<String>()
            omitEmptyStrings -> Splitter.on(delimiter).trimResults().omitEmptyStrings().let {
                if (limit == 0) it else it.limit(limit)
            }.splitToList(rest)
            else -> rest.split(delimiter, limit = limit)
        }
        if (fields.size < fieldsNumber - 3) {
            throw BedEntryUnpackException(this, (fields.size + 3).toByte(), "field is missing")
        }

        val name = if (fieldsNumber >= 4) fields[0] else "."
        val score = when {
            fieldsNumber >= 5 -> {
                val chunk = fields[1]
                if (chunk == ".") {
                    0
                } else {
                    chunk.toIntOrNull()
                            ?: throw BedEntryUnpackException(this, 4, "score value $chunk is not an integer")
                }
            }
            else -> 0
        }
        val strand = if (fieldsNumber >= 6) fields[2].firstOrNull() ?: '.' else '.'
        val thickStart = when {
            fieldsNumber >= 7 -> {
                val chunk = fields[3]
                if (chunk == ".") {
                    0
                } else {
                    chunk.toIntOrNull()
                            ?: throw BedEntryUnpackException(this, 6, "thickStart value $chunk is not an integer")
                }
            }
            else -> 0
        }
        val thickEnd = when {
            fieldsNumber >= 8 -> {
                val chunk = fields[4]
                if (chunk == ".") {
                    0
                } else {
                    chunk.toIntOrNull()
                            ?: throw BedEntryUnpackException(this, 7, "thickEnd value $chunk is not an integer")
                }
            }
            else -> 0
        }
        val color = if (fieldsNumber >= 9) {
            val value = fields[5]
            if (value == "0" || value == ".") {
                0
            } else {
                try {
                    val chunks = value.split(',', limit = 3)
                    Color(chunks[0].toInt(), chunks[1].toInt(), chunks[2].toInt()).rgb
                } catch (e: Exception) {
                    throw BedEntryUnpackException(this, 8, "color value $value is not a comma-separated RGB", e)
                }
            }
        } else {
            0
        }
        val blockCount = when {
            fieldsNumber >= 10 -> {
                val chunk = fields[6]
                if (chunk == ".") {
                    0
                } else {
                    chunk.toIntOrNull()
                            ?: throw BedEntryUnpackException(this, 9, "blockCount value $chunk is not an integer")
                }
            }
            else -> 0
        }

        val blockSizes = if (fieldsNumber >= 11) {
            val value = fields[7]
            if (blockCount > 0) {
                try {
                    value.splitToInts(blockCount)
                } catch (e: Exception) {
                    throw BedEntryUnpackException(
                        this, 10,
                        "blockSizes value $value is not a comma-separated integer list of size $blockCount", e
                    )
                }
            } else null
        } else null

        val blockStarts = if (fieldsNumber >= 12) {
            val value = fields[8]
            if (blockCount > 0) {
                try {
                    value.splitToInts(blockCount)
                } catch (e: Exception) {
                    throw BedEntryUnpackException(
                        this, 11,
                        "blockStarts value $value is not a comma-separated integer list of size $blockCount", e
                    )
                }
            } else null
        } else null


        val actualExtraFieldsNumber = if (parseExtraFields) fields.size - fieldsNumber + 3 else 0

        val extraFields = if (actualExtraFieldsNumber != 0) {
            val parsedExtraFields = Array(actualExtraFieldsNumber) { i -> fields[fieldsNumber - 3 + i] }
            // this specific check is intended to exactly replicate the original behaviour:
            // extraFields are null if the bed entry tail is an empty string.
            // see e.g. [BedEntryTest.unpackBedEmptyExtraFields2]
            if (actualExtraFieldsNumber > 1 || parsedExtraFields[0] != "") parsedExtraFields else null
        } else {
            null
        }

        return ExtendedBedEntry(
            chrom, start, end,
            if (name == "") "." else name,
            score, strand, thickStart, thickEnd, color,
            blockCount, blockSizes, blockStarts, extraFields
        )
    }

    @Deprecated(
        "use parseExtraFields instead of extraFieldsNumber",
        ReplaceWith("unpack(fieldsNumber, extraFieldsNumber != 0, delimiter, omitEmptyStrings)")
    )
    fun unpack(
            fieldsNumber: Byte = 12,
            extraFieldsNumber: Int?,
            delimiter: Char = '\t',
            omitEmptyStrings: Boolean = false
    ) = unpack(fieldsNumber, extraFieldsNumber != 0, delimiter, omitEmptyStrings)


    private fun String.splitToInts(size: Int): IntArray {
        val chunks = IntArray(size)
        val s = Splitter.on(',').split(this).iterator()
        var ptr = 0
        while (ptr < size) {
            chunks[ptr++] = s.next().toInt()
        }
        return chunks
    }
}

/**
 * An extended representation of a BED file entry.
 *
 * The BED standard allows 3 up to 12 regular fields (bed3 through bed12) and an arbitrary number
 * of custom extra fields (bedN+K format). The first 12 properties represent the regular fields (default values
 * are used to stand in for the missing data). [extraFields] property stores the extra fields as a [String] array.
 */
data class ExtendedBedEntry(
        /** Chromosome name, e.g. `"chr9"`. */
        val chrom: String,
        /** 0-based start offset (inclusive). */
        val start: Int,
        /** 0-based end offset (exclusive). */
        val end: Int,
        /** Name of feature. */
        val name: String = ".",
    // UCSC defines score as an integer in range [0,1000], but almost everyone ignores the range.
        /** Feature score */
        val score: Int = 0,
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
        // Unfortunately MACS2 generates *.bed files with score > 1000
        // let's ignore this check:
        // require(score in 0..1000) {
        //     "Unexpected score: $score"
        // }

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
        result = 31 * result + score.hashCode()
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
     * Convert to [BedEntry].
     *
     * Intended as an inverse for [BedEntry.unpack]. Packs the optional fields (every field except the obligatory
     * first three ones, chrom, start and end) in [BedEntry.rest].
     *
     * @param fieldsNumber BED format fields number to serialize (3..12)
     * @param extraFieldsNumber BED+ format extra fields number to serialize, if null serialize all extra fields
     * @param delimiter Custom delimiter for malformed data
     */
    fun pack(
            fieldsNumber: Byte = 12,
            extraFieldsNumber: Int? = null,
            delimiter: Char = '\t'
    ): BedEntry {
        check(fieldsNumber in 3..12) { "Expected fields number in range 3..12, but received $fieldsNumber" }

        return BedEntry(
            chrom, start, end,
            rest(fieldsNumber, extraFieldsNumber).joinToString(delimiter.toString())
        )
    }

    /**
     * List of optional fields (all except the obligatory first three) for BED entry,
     * same fields as in [BedEntry.rest] after [pack].
     *
     * Values in string differs from original values because converted to string.
     *
     * @param fieldsNumber BED format fields number to serialize (3..12)
     * @param extraFieldsNumber BED+ format extra fields number to serialize, if null serialize all extra fields
     */
    fun rest(fieldsNumber: Byte = 12, extraFieldsNumber: Int? = null): ArrayList<String> {
        check(fieldsNumber in 3..12) { "Fields number expected 3..12, but was $fieldsNumber" }

        val rest = ArrayList<String>()

        if (fieldsNumber >= 4) {
            // Empty string will lead to incorrect results
            rest.add(if (name.isNotEmpty()) name else ".")
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
        return rest
    }

    /**
     * Returns a i-th field of a Bed entry.
     *
     * Since [ExtendedBedEntry] is format-agnostic, it doesn't actually know which field is i-th,
     * so we have to provide [fieldsNumber] and [extraFieldsNumber].
     * Returns an instance of a correct type ([Int], [String] etc.) or null for missing and out of bounds fields.
     * This method is useful for minimizing the number of conversions to and from [String].
     *
     * @param i the index of the field being queried (zero-based)
     * @param fieldsNumber the number of regular BED fields (N in bedN+K notation)
     * @param extraFieldsNumber the number of extra BED fields (0 for bedN, K for bedN+K, null for bedN+).
     * The extra fields are always returned as [String].
     */
    fun getField(i: Int, fieldsNumber: Int = 12, extraFieldsNumber: Int? = null): Any? {
        val actualExtraFieldsNumber = extraFieldsNumber ?: extraFields?.size ?: 0
        return when {
            i >= fieldsNumber + actualExtraFieldsNumber -> null
            i >= fieldsNumber -> extraFields?.let {
                if (i - fieldsNumber < it.size) it[i - fieldsNumber] else null
            }
            else -> when (i) {
                0 -> chrom
                1 -> start
                2 -> end
                3 -> name
                4 -> score
                5 -> strand
                6 -> thickStart
                7 -> thickEnd
                8 -> itemRgb
                9 -> blockCount
                10 -> blockSizes
                11 -> blockStarts
                else -> null
            }
        }
    }
}

class BedEntryUnpackException(
        val entry: BedEntry, val fieldIdx: Byte, reason: String, cause: Throwable? = null
) : Exception("Unpacking BED entry failed at field ${fieldIdx + 1}. Reason: $reason", cause)
