package org.jbb.big

import com.google.common.base.MoreObjects
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import com.google.common.collect.UnmodifiableIterator
import gnu.trove.list.TFloatList
import gnu.trove.list.TIntList
import gnu.trove.list.array.TFloatArrayList
import gnu.trove.list.array.TIntArrayList
import java.io.*
import java.nio.file.Path
import java.util.ArrayList
import java.util.HashMap
import java.util.NoSuchElementException
import java.util.Objects
import kotlin.platform.platformStatic

/**
 * Bigger brother of the good-old WIG format.
 */
public class BigWigFile throws(IOException::class) protected constructor(path: Path) :
        BigFile<WigSection>(path, magic = 0x888FFC26.toInt()) {

    throws(IOException::class)
    override fun queryInternal(dataOffset: Long, dataSize: Long,
                               query: ChromosomeInterval): Sequence<WigSection> {
        val chrom = chromosomes[query.chromIx]
        return listOf(input.with(dataOffset, dataSize, compressed) {
            assert(readInt() == query.chromIx, "section contains wrong chromosome")
            val start = readInt()
            readInt()   // end.
            val step = readInt()
            val span = readInt()
            val type = readUnsignedByte()
            readByte()  // reserved.
            val count = readUnsignedShort()

            val types = WigSection.Type.values()
            check(type >= 1 && type <= types.size())
            when (types[type - 1]) {
                WigSection.Type.BED_GRAPH ->
                    throw IllegalStateException("bedGraph sections aren't supported")
                WigSection.Type.VARIABLE_STEP -> {
                    val section = VariableStepSection(chrom, span)
                    for (i in 0 until count) {
                        val position = readInt()
                        section[position] = readFloat()
                    }

                    section
                }
                WigSection.Type.FIXED_STEP -> {
                    val section = FixedStepSection(chrom, start, step, span)
                    for (i in 0 until count) {
                        section.add(readFloat())
                    }

                    section
                }
            }
        }).asSequence()
    }

    companion object {
        throws(IOException::class)
        public platformStatic fun read(path: Path): BigWigFile = BigWigFile(path)
    }
}

/**
 * A basic WIG format parser.
 *
 * According to the spec. WIG positions are *always* 1-based, thus we manually
 * convert them to 0-based during parsing.
 *
 * See http://genome.ucsc.edu/goldenPath/help/wiggle.html
 *
 * @author Sergei Lebedev
 * @since 23/06/15
 */
public class WigParser(private val reader: Reader) :
        Iterable<Pair<String, WigSection>>, AutoCloseable, Closeable {
    override fun iterator(): UnmodifiableIterator<Pair<String, WigSection>> {
        return WigIterator(reader.buffered())
    }

    override fun close() = reader.close()
}

private fun Sequence<Pair<K, V>>.toMap<K, V>(): Map<K, V> {
    val acc = HashMap<K, V>()
    for (val (k, v) in this) {
        acc[k] = v
    }

    return acc
}

private class WigIterator(private val reader: BufferedReader) :
        UnmodifiableIterator<Pair<String, WigSection>>() {

    private var lines: PeekingIterator<String> =
            Iterators.peekingIterator(reader.lines().iterator())
    private var cached: Pair<String, WigSection>? = null

    override fun hasNext(): Boolean {
        if (cached == null) {
            cached = cache()  // Got some?
        }

        return cached != null
    }

    override fun next(): Pair<String, WigSection>? {
        check(hasNext())
        val next = cached
        cached = null
        return next
    }

    private fun cache(): Pair<String, WigSection>? {
        var chromosome: String? = null
        var track: WigSection? = null
        var state = State.WAITING
        loop@ while (lines.hasNext()) {
            val line = lines.peek()
            when {
                line.startsWith('#') ->
                    continue@loop  // Skip comments.
                state != State.WAITING && !RE_VALUE.matches(line) ->
                    break@loop     // My job here is done.
            }

            val chunks = RE_WHITESPACE.split(line, 2)
            when (state) {
                State.WAITING -> {
                    val (type, rest) = chunks
                    val params = RE_PARAM.matchAll(rest).map { m ->
                        (m.groups[1]!!.value to
                                m.groups[2]!!.value.removeSurrounding("\""))
                    }.toMap()

                    when (type) {
                        "track" -> check(params["type"] == "wiggle_0")
                        "variableStep" -> state = State.VARIABLE_STEP
                        "fixedStep" -> state = State.FIXED_STEP
                        else -> assert(false)
                    }

                    if (state != State.WAITING) {
                        chromosome = params["chrom"]
                        track = state.create(params)
                    }
                }
                State.VARIABLE_STEP -> (track as VariableStepSection)
                        .set(chunks[0].toInt() - 1, chunks[1].toFloat())
                State.FIXED_STEP -> (track as FixedStepSection)
                        .add(chunks[0].toFloat())
            }

            lines.next()  // Discard the processed line.
        }

        return if (chromosome != null && track != null) {
            chromosome to track
        } else {
            null
        }
    }

    companion object {
        val RE_VALUE = "(\\d+)?\\s*[+-]?(\\d+(?:\\.\\d+)?(e\\+?\\d+)?)?".toRegex()
        val RE_WHITESPACE = "\\s".toRegex()
        val RE_PARAM = "(\\S+)=(\"[^\"]*\"|\\S+)".toRegex()
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

public class WigPrinter jvmOverloads constructor(
        private val writer: Writer,
        private val name: String,
        private val description: String = name) : Closeable, AutoCloseable {

    init {
        writer.write("track type=wiggle_0 name=\"$name\" description=\"$description\"\n")
    }

    public fun print(track: VariableStepSection) {
        writer.write("variableStep chrom=${track.chrom} span=${track.span}\n")

        for (range in track.query()) {
            writer.write("${range.startOffset + 1} ${range.score}\n")
        }
    }

    public fun print(track: FixedStepSection) {
        writer.write("fixedStep chrom=${track.chrom} " +
                     "start=${track.start + 1} " +
                     "step=${track.step} span=${track.span}\n")

        for (range in track.query()) {
            writer.write("${range.score}\n")
        }
    }

    override fun close() = writer.close()
}


public interface WigSection {
    val chrom: String
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

    /**
     * Returns a list with all intervals in the section.
     */
    public fun query(): List<WigInterval> = query(start, end)

    /**
     * Returns a list of intervals contained within a given semi-interval.
     *
     * @param from inclusive
     * @param to exclusive
     */
    public fun query(from: Int, to: Int): List<WigInterval>

    public enum class Type(public val id: Int) {
        BED_GRAPH(1),
        VARIABLE_STEP(2),
        FIXED_STEP(3)
    }
}

/**
 * A section with gaps; _variable_ step mean i+1-th range is on
 * arbitrary distance from i-th range. Note, however, that range
 * width remains _fixed_ throughout the track.
 */
public class VariableStepSection(
        public override val chrom: String,
        /** Range width. */
        public val span: Int = 1) : WigSection {

    override val start: Int get() {
        return if (positions.isEmpty()) 0 else positions[0]
    }

    override val end: Int get() = if (positions.isEmpty()) {
        Integer.MAX_VALUE
    } else {
        positions[positions.size() - 1] + span
    }

    private val positions: TIntList = TIntArrayList()
    private val values: TFloatList = TFloatArrayList()

    public fun set(pos: Int, value: Float) {
        val i = positions.binarySearch(pos)
        if (i < 0) {
            positions.insert(i.inv(), pos)
            values.insert(i.inv(), value)
        } else {
            values[i] += value
        }
    }

    public fun get(pos: Int): Float {
        val i = positions.binarySearch(pos)
        if (i < 0) {
            throw NoSuchElementException()
        }

        return values[i]
    }

    override fun query(from: Int, to: Int): List<WigInterval> {
        var acc = ArrayList<WigInterval>()
        var i = positions.binarySearch(from)
        if (i < 0) {
            i = i.inv()
        }

        while (i < positions.size() && positions[i] <= to - span) {
            acc.add(WigInterval(positions[i], positions[i] + span, values[i]))
            i++
        }

        return acc
    }

    override fun toString(): String = MoreObjects.toStringHelper(this)
            .addValue(span)
            .toString()

    override fun equals(other: Any?): Boolean = when {
        other identityEquals this -> true
        other !is VariableStepSection -> false
        else -> span == other.span &&
                positions == other.positions &&
                values == other.values
    }

    override fun hashCode(): Int = Objects.hash(span, positions, values)
}

/**
 * A section with contiguous ranges. Both the distance between consecutive
 * ranges and range width is fixed throughout the track.
 */
public class FixedStepSection(
        public override val chrom: String,
        /** Start offset of the first range on the track. */
        public override val start: Int,
        /** Distance between consecutive ranges. */
        public val step: Int = 1,
        /** Range width. */
        public val span: Int = 1) : WigSection {

    override val end: Int get() = start + step * (values.size() - 1) + span

    private val values: TFloatList = TFloatArrayList()

    public fun add(value: Float) {
        values.add(value)
    }

    public fun get(pos: Int): Float {
        // Note(lebedev): we expect 'pos' to be a starting position.
        return values[(pos - start) / step]
    }

    override fun query(from: Int, to: Int): List<WigInterval> {
        val ranges = ArrayList<WigInterval>()
        var i = Math.max(start, from - from % span)
        while (i < Math.min(end, to - to % span)) {
            ranges.add(WigInterval(i, i + span, get(i)))
            i += step
        }

        return ranges
    }

    override fun toString(): String = MoreObjects.toStringHelper(this)
            .add("start", start)
            .add("step", step)
            .add("span", span)
            .toString()

    override fun equals(other: Any?): Boolean = when {
        other identityEquals this -> true
        other !is FixedStepSection -> false
        else -> start == start &&
                step == other.step && span == other.span &&
                values == other.values
    }

    override fun hashCode(): Int = Objects.hash(start, step, span, values)
}

public data class WigInterval(val startOffset: Int,
                              val endOffset: Int,
                              val score: Float)