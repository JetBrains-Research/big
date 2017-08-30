package org.jetbrains.bio.big

import com.google.common.annotations.VisibleForTesting
import com.google.common.base.CharMatcher
import com.google.common.base.MoreObjects
import com.google.common.base.Splitter
import com.google.common.collect.ComparisonChain
import gnu.trove.list.TFloatList
import gnu.trove.list.TIntList
import gnu.trove.list.array.TFloatArrayList
import gnu.trove.list.array.TIntArrayList
import org.jetbrains.bio.*
import java.io.BufferedReader
import java.io.Closeable
import java.io.Writer
import java.nio.file.Path
import java.util.*

/**
 * A basic WIG format parser.
 *
 * According to the spec. WIG positions are *always* 1-based, thus we manually
 * convert them to 0-based during parsing.
 *
 * See http://genome.ucsc.edu/goldenPath/help/wiggle.html
 */
class WigFile(private val path: Path) : Iterable<WigSection> {
    override fun iterator(): Iterator<WigSection> = WigIterator(path.bufferedReader())
}

@VisibleForTesting
internal class WigIterator(reader: BufferedReader) : CachingIterator<WigSection>(reader) {
    @Suppress("platform_class_mapped_to_kotlin")
    override fun cache(): WigSection? {
        var track: WigSection? = null
        var state = State.WAITING
        loop@ while (lines.hasNext()) {
            // Java trimming does not allocate a new string if there's
            // nothing to trim.
            val line = (lines.peek() as java.lang.String).trim()
            when {
                line.startsWith('#') -> {
                    lines.next()
                    continue@loop  // Skip comments.
                }
                state != State.WAITING && META_MATCHER.matches(line.first()) ->
                    break@loop     // My job here is done.
            }

            val chunks = LINE_SPLITTER.split(line).iterator()
            when (state) {
                State.WAITING -> {
                    val type = chunks.next()
                    val rest = chunks.next()
                    val params = RE_PARAM.findAll(rest).map { m ->
                        (m.groups[1]!!.value to
                                m.groups[2]!!.value.removeSurrounding("\""))
                    }.toMap()

                    when (type) {
                        "track" -> check(params["type"] == "wiggle_0")
                        "variableStep" -> state = State.VARIABLE_STEP
                        "fixedStep" -> state = State.FIXED_STEP
                        "browser" -> { /* Just ignore */ }
                        else -> error("Unexpected section: $type")
                    }

                    if (state != State.WAITING) {
                        track = state.create(params)
                    }
                }
                State.VARIABLE_STEP -> (track as VariableStepSection)
                        .set(chunks.next().toInt() - 1, chunks.next().toFloat())
                State.FIXED_STEP -> (track as FixedStepSection)
                        .add(chunks.next().toFloat())
            }

            lines.next()  // Discard the processed line.
        }

        return track
    }

    companion object {
        /** Not a 'Regex' to avoid excessive allocation on '.split'. */
        private val LINE_SPLITTER = Splitter.on(CharMatcher.whitespace()).limit(2)
        /**
         * Matches prefixes of meta-data segments "track", "variableStep"
         * and "fixedStep".
         */
        private val META_MATCHER = CharMatcher.anyOf("tvf")
        private val RE_PARAM = "(\\S+)=(\"[^\"]*\"|\\S+)".toRegex()
    }
}

/**
 * Parser state.
 */
private enum class State {
    /** Waiting for the track definition line. */
    WAITING {
        override fun create(params: Map<String, String>): WigSection {
            throw UnsupportedOperationException()
        }
    },

    /** Parsing a variable step track. */
    VARIABLE_STEP {
        override fun create(params: Map<String, String>): VariableStepSection {
            val span = params["span"]?.toInt() ?: 1
            return VariableStepSection(params["chrom"]!!, span)
        }
    },

    /** Parsing a fixed step track. */
    FIXED_STEP {
        override fun create(params: Map<String, String>): FixedStepSection {
            val start = params["start"]!!.toInt() - 1
            val step = params["step"]?.toInt() ?: 1
            val span = params["span"]?.toInt() ?: 1
            return FixedStepSection(params["chrom"]!!, start, step, span)
        }
    };

    abstract fun create(params: Map<String, String>): WigSection
}

class WigPrinter @JvmOverloads constructor(
        private val writer: Writer,
        private val name: String,
        description: String = name) : Closeable, AutoCloseable {

    init {
        writer.write("track type=wiggle_0 name=\"$name\" description=\"$description\"\n")
    }

    fun print(track: VariableStepSection) {
        writer.write("variableStep chrom=${track.chrom} span=${track.span}\n")

        for ((start, _/* end */, score) in track.query()) {
            writer.write("${start + 1} $score\n")
        }
    }

    fun print(track: FixedStepSection) {
        writer.write("fixedStep chrom=${track.chrom} " +
                     "start=${track.start + 1} " +
                     "step=${track.step} span=${track.span}\n")

        for (interval in track.query()) {
            writer.write("${interval.score}\n")
        }
    }

    override fun close() = writer.close()
}


interface WigSection : Comparable<WigSection> {
    /** Human-readable chromosome name, e.g. "`chr`". */
    val chrom: String

    /** Interval width. */
    val span: Int

    /**
     * Start offset of the leftmost interval in the section.
     */
    val start: Int

    /**
     * End offset of the rightmost interval in the section. Note that
     * we use semi-closed intervals, thus the returned value is not
     * _included_ on the track.
     */
    val end: Int

    /** The total number of intervals in the section. */
    val size: Int

    fun isEmpty() = size == 0

    /**
     * Returns a list with all intervals in the section.
     */
    fun query(): Sequence<ScoredInterval> {
        return if (isEmpty()) emptySequence() else query(start, end)
    }

    /**
     * Returns a intervals contained within a given semi-interval.
     *
     * @param from inclusive
     * @param to exclusive
     */
    fun query(from: Int, to: Int): Sequence<ScoredInterval>

    /**
     * Splices a section into sub-section of size at most [java.lang.Short.MAX_VALUE].
     */
    fun splice(max: Int = Short.MAX_VALUE.toInt()): Sequence<WigSection>

    override fun compareTo(other: WigSection): Int = ComparisonChain.start()
            .compare(chrom, other.chrom)
            .compare(start, other.start)
            .result()

    enum class Type {
        BED_GRAPH,
        VARIABLE_STEP,
        FIXED_STEP
    }
}

/**
 * A section with gaps; _variable_ step mean i+1-th interval is on
 * arbitrary distance from i-th interval. Note, however, that interval
 * width remains _fixed_ throughout the track.
 */
data class VariableStepSection(
        override val chrom: String,
        override val span: Int = 1,
        /** Per-interval positions. */
        internal val positions: TIntList = TIntArrayList(),
        /** Per-interval values. */
        internal val values: TFloatList = TFloatArrayList()) : WigSection {

    override val size: Int get() = values.size()

    init {
        require(positions.size() == values.size())
    }

    override val start: Int get() {
        check(size > 0) { "no data" }
        return positions[0]
    }

    override val end: Int get() {
        check(size > 0) { "no data" }
        return positions.last() + span
    }

    operator fun set(pos: Int, value: Float) {
        if (positions.isEmpty || pos > positions.last()) {
            positions.add(pos)
            values.add(value)
        } else {
            val i = positions.binarySearch(pos)
            if (i < 0) {
                positions.insert(i.inv(), pos)
                values.insert(i.inv(), value)
            } else {
                values[i] += value
            }
        }
    }

    operator fun get(pos: Int): Float {
        val i = positions.binarySearch(pos)
        if (i < 0) {
            throw NoSuchElementException()
        }

        return values[i]
    }

    override fun query(from: Int, to: Int): Sequence<ScoredInterval> {
        var i = positions.binarySearch(from)
        if (i < 0) {
            i = i.inv()
        }

        var j = positions.binarySearch(to - span + 1)
        if (j < 0) {
            j = j.inv() - 1
        }

        return (i..j).asSequence()
                .map { ScoredInterval(positions[it], positions[it] + span, values[it]) }
    }

    override fun splice(max: Int): Sequence<VariableStepSection> {
        val chunks = size divCeiling max
        return if (chunks == 1) {
            sequenceOf(this)
        } else {
            (0 until chunks).mapUnboxed { i ->
                val from = i * max
                val to = Math.min((i + 1) * max, size)
                copy(positions = positions.subList(from, to),
                     values = values.subList(from, to))
            }
        }
    }

    override fun toString() = MoreObjects.toStringHelper(this)
            .addValue(chrom)
            .addValue(span)
            .toString()

    override fun equals(other: Any?) = when {
        other === this -> true
        other !is VariableStepSection -> false
        else -> span == other.span &&
                positions == other.positions &&
                values == other.values
    }

    override fun hashCode() = Objects.hash(span, positions, values)
}

/**
 * A section with contiguous interval. Both the distance between
 * consecutive intervals and interval width is fixed throughout the
 * section.
 */
data class FixedStepSection(
        override val chrom: String,
        override val start: Int,
        /** Distance between consecutive intervals. */
        val step: Int = 1,
        override val span: Int = 1,
        /** Per-interval values. */
        internal val values: TFloatList = TFloatArrayList()) : WigSection {

    override val end: Int get() = start + step * (values.size() - 1) + span

    override val size: Int get() = values.size()

    fun add(value: Float) = ignore(values.add(value))

    operator fun get(pos: Int): Float {
        // Note(lebedev): we expect 'pos' to be a starting position.
        return values[(pos - start) / step]
    }

    override fun query(from: Int, to: Int): Sequence<ScoredInterval> {
        val i = Math.max(start, from - from % span)
        val j = Math.min(start + step * size, to - to % span)
        return (i until j step step)
                .mapUnboxed { ScoredInterval(it, it + span, get(it)) }
    }

    override fun splice(max: Int): Sequence<FixedStepSection> {
        val chunks = size divCeiling max
        return if (chunks == 1) {
            sequenceOf(this)
        } else {
            (0 until chunks).mapUnboxed { i ->
                val from = i * max
                val to = Math.min((i + 1) * max, values.size())
                copy(start = start + step * from, values = values.subList(from, to))
            }
        }
    }

    override fun toString() = MoreObjects.toStringHelper(this)
            .addValue(chrom)
            .add("start", start)
            .add("end", end)
            .add("step", step)
            .add("span", span)
            .toString()

    override fun equals(other: Any?) = when {
        other === this -> true
        other !is FixedStepSection -> false
        else -> start == start &&
                step == other.step && span == other.span &&
                values == other.values
    }

    override fun hashCode() = Objects.hash(start, step, span, values)
}

private fun TIntList.last() = get(size() - 1)