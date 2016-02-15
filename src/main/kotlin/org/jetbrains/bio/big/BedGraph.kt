package org.jetbrains.bio.big

import com.google.common.base.MoreObjects
import com.google.common.primitives.Ints
import gnu.trove.list.TFloatList
import gnu.trove.list.TIntList
import gnu.trove.list.array.TFloatArrayList
import gnu.trove.list.array.TIntArrayList
import org.apache.commons.math3.stat.descriptive.moment.Mean
import org.jetbrains.bio.CachingIterator
import org.jetbrains.bio.ScoredInterval
import org.jetbrains.bio.divCeiling
import org.jetbrains.bio.mapUnboxed
import java.io.BufferedReader
import java.io.Closeable
import java.io.Reader
import java.util.*

/**
 * A basic BedGraph format parser.
 *
 * Separated from [WigParser] for clarity.
 *
 * See http://genome.ucsc.edu/goldenpath/help/bedgraph.html
 */
class BedGraphParser(private val reader: Reader) :
        Iterable<BedGraphSection>, AutoCloseable, Closeable {

    override fun iterator(): Iterator<BedGraphSection> = BedGraphIterator(reader.buffered())

    override fun close() = reader.close()
}

private class BedGraphIterator(reader: BufferedReader) :
        CachingIterator<BedGraphSection>(reader) {

    private var waiting = true

    override fun cache(): BedGraphSection? {
        var track: BedGraphSection? = null
        loop@ while (lines.hasNext()) {
            val line = lines.peek().trim()
            if (line.isEmpty() || line.startsWith('#')) {
                // Skip blank lines and comments.
            } else if (waiting) {
                val (type, rest) = RE_WHITESPACE.split(line, 2)
                val params = RE_PARAM.findAll(rest).map { m ->
                    (m.groups[1]!!.value to
                            m.groups[2]!!.value.removeSurrounding("\""))
                }.toMap()

                check(type == "track" && params["type"] == "bedGraph")
                waiting = false
            } else {
                val (chrom, start, end, value) = RE_WHITESPACE.split(line, 4)
                if (track == null) {
                    track = BedGraphSection(chrom)
                } else if (track.chrom != chrom) {
                    break@loop
                }

                track[start.toInt(), end.toInt()] = value.toFloat()
            }

            lines.next()
        }

        return track
    }

    companion object {
        private val RE_WHITESPACE = "\\s".toRegex()
        private val RE_PARAM = "(\\S+)=(\"[^\"]*\"|\\S+)".toRegex()
    }
}

/**
 * A section for variable step / variable span intervals.
 *
 * Even though BedGraph is a separate format it is allowed in BigWIG
 * data section.
 * */
data class BedGraphSection(
        override val chrom: String,
        /** Per-interval start positions. */
        internal val startOffsets: TIntList = TIntArrayList(),
        /** Per-interval end positions. */
        internal val endOffsets: TIntList = TIntArrayList(),
        /** Per-interval values. */
        internal val values: TFloatList = TFloatArrayList()) : WigSection {

    override val span: Int get() {
        val mean = Mean()
        for (i in 0..size() - 1) {
            mean.increment((endOffsets[i] - startOffsets[i]).toDouble())
        }

        return Ints.saturatedCast(Math.round(mean.result))
    }

    override val start: Int get() {
        check(size() > 0) { "no data" }
        return startOffsets[0]
    }

    override val end: Int get() {
        check(size() > 0) { "no data" }
        // XXX intervals might overlap, so in general we don't know the
        // rightmost offset.
        return endOffsets.max()
    }

    operator fun set(startOffset: Int, endOffset: Int, value: Float) {
        val i = startOffsets.binarySearch(startOffset)
        when {
            i < 0 -> {
                startOffsets.insert(i.inv(), startOffset)
                endOffsets.insert(i.inv(), endOffset)
                values.insert(i.inv(), value)
            }
            endOffsets[i] != endOffset -> {
                startOffsets.insert(i + 1, startOffset)
                endOffsets.insert(i + 1, endOffset)
                values.insert(i + 1, value)
            }
            else -> {
                values[i] += value
            }
        }
    }

    operator fun get(startOffset: Int, endOffset: Int): Float {
        val i = startOffsets.binarySearch(startOffset)
        if (i < 0 || endOffsets[i] != endOffset) {
            throw NoSuchElementException()
        }

        return values[i]
    }

    override fun query(from: Int, to: Int): Sequence<ScoredInterval> {
        var i = startOffsets.binarySearch(from)
        if (i < 0) {
            i = i.inv()
        }

        var j = startOffsets.binarySearch(to + 1)
        if (j < 0) {
            j = j.inv() - 1
        }

        return (i..j).asSequence()
                .filter { endOffsets[it] <= to }
                .map { ScoredInterval(startOffsets[it], endOffsets[it], values[it]) }
    }

    override fun splice(max: Int): Sequence<WigSection> {
        val chunks = size() divCeiling max
        return if (chunks == 1) {
            sequenceOf(this)
        } else {
            (0..chunks - 1).mapUnboxed { i ->
                val from = i * max
                val to = Math.min((i + 1) * max, size())
                copy(startOffsets = startOffsets.subList(from, to),
                     endOffsets = endOffsets.subList(from, to),
                     values = values.subList(from, to))
            }
        }
    }

    override fun size() = values.size()

    override fun toString() = MoreObjects.toStringHelper(this)
            .addValue(chrom)
            .toString()
}
