package org.jetbrains.bio.big

import com.google.common.base.Stopwatch
import com.google.common.collect.ComparisonChain
import com.google.common.collect.Iterators
import com.google.common.collect.Ordering
import com.google.common.math.IntMath
import org.apache.log4j.Logger
import java.io.BufferedReader
import java.math.RoundingMode
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream

/**
 * Various internal helpers.
 *
 * You shouldn't be using them outside of `big`.
 */

fun Int.divCeiling(other: Int) = IntMath.divide(this, other, RoundingMode.CEILING)

// Remove once KT-8248 is done.
fun Int.pow(other: Int) = IntMath.pow(this, other)

/**
 * Computes the value n such that base^n <= this.
 */
fun Int.logCeiling(base: Int): Int {
    require(this > 0, "non-positive number")
    require(base > 1, "base must be >1")

    var rem = this
    var acc = 1
    while (rem > base) {
        rem = rem divCeiling base
        acc++
    }

    return acc
}

// Remove once KT-4665 is done.
fun Int.until(other: Int) = this..other - 1

// Remove once KT-8872 is done.
fun IntRange.by(step: Int) = step(step)

fun String.trimZeros() = trimEnd { it == '\u0000' }

fun Path.bufferedReader(vararg options: OpenOption): BufferedReader {
    val inputStream = Files.newInputStream(this, *options).buffered()
    return when (toFile().extension) {
        "gz"  -> GZIPInputStream(inputStream)
        "zip" -> ZipInputStream(inputStream)
        else  -> inputStream
    }.bufferedReader()
}

inline fun withTempFile(prefix: String, suffix: String,
                        block: (Path) -> Unit) {
    val path = Files.createTempFile(prefix, suffix)
    try {
        block(path)
    } finally {
        Files.delete(path)
    }
}

/** Fetches chromosome sizes from a UCSC provided TSV file. */
fun Path.chromosomes(): List<BPlusLeaf> {
    return bufferedReader().lineSequence().mapIndexed { i, line ->
        val chunks = line.split('\t', limit = 3)
        BPlusLeaf(chunks[0], i, chunks[1].toInt())
    }.toList()
}

fun Sequence<T>.partition<T>(n: Int): Sequence<Iterable<T>> {
    require(n > 0, "n must be >0")
    val that = this
    return object : Sequence<Iterable<T>> {
        override fun iterator(): Iterator<Iterable<T>> {
            return Iterators.partition(that.iterator(), n)
        }
    }
}

inline fun Logger.time(message: String, block: () -> Unit) {
    debug(message)
    val stopwatch = Stopwatch.createStarted()
    block()
    stopwatch.stop()
    debug("Done in $stopwatch")
}

/**
 * A semi-closed interval.
 */
interface Interval {
    /** Start offset (inclusive).  */
    public val left: Offset
    /** End offset (exclusive).  */
    public val right: Offset

    public fun intersects(other: Interval): Boolean {
        return !(other.right <= left || other.left >= right)
    }

    /**
     * Returns a union of the two intervals, i.e. an interval which
     * completely covers both of them.
     */
    public fun union(other: Interval): Interval {
        val ord = Ordering.natural<Offset>()
        val unionLeft = ord.min(left, other.left)
        val unionRight = ord.max(right, other.right)
        return Interval(unionLeft.chromIx, unionLeft.offset,
                        unionRight.chromIx, unionRight.offset)
    }

    override fun toString(): String = "[$left; $right)"

    companion object {
        fun invoke(chromIx: Int, startOffset: Int, endOffset: Int): ChromosomeInterval {
            require(startOffset < endOffset) {
                "start must be <end, got [$startOffset, $endOffset)"
            }

            return ChromosomeInterval(chromIx, startOffset, endOffset)
        }

        fun invoke(startChromIx: Int, startOffset: Int,
                   endChromIx: Int, endOffset: Int): Interval {
            return if (startChromIx == endChromIx) {
                invoke(startChromIx, startOffset, endOffset)
            } else {
                MultiInterval(Offset(startChromIx, startOffset),
                              Offset(endChromIx, endOffset))
            }

        }
    }
}

/** An interval on a chromosome. */
data open class ChromosomeInterval(public val chromIx: Int,
                                   public val startOffset: Int,
                                   public val endOffset: Int) : Interval {
    override val left: Offset get() = Offset(chromIx, startOffset)
    override val right: Offset get() = Offset(chromIx, endOffset)

    /** Checks if a given interval is contained in this interval. */
    public fun contains(other: ChromosomeInterval): Boolean {
        return other.chromIx == chromIx &&
               other.startOffset >= startOffset &&
               other.endOffset <= endOffset
    }

    /**
     * Returns an intersection of the two intervals, i.e. an interval which
     * is completely contained in both of them.
     */
    public fun intersection(other: ChromosomeInterval): ChromosomeInterval {
        return Interval(chromIx,
                        Math.max(startOffset, other.startOffset),
                        Math.min(endOffset, other.endOffset))
    }

    /**
     * Produces a sequence of `n` sub-intervals.
     *
     * The behaviour for the case of `length() % n > 0` is unspecified.
     * However, the sub-intervals are guaranteed to be disjoint and
     * cover the whole interval.
     */
    fun slice(n: Int): Sequence<ChromosomeInterval> {
        require(n > 0, "n must be >0")
        require(n <= length()) { "n must be <= length, got $n > ${length()}" }
        return if (n == 1) {
            sequenceOf(this)
        } else {
            val width = length().toDouble() / n
            (0 until n).asSequence().map { i ->
                val start = Math.round(startOffset + i * width).toInt()
                val end = Math.round(startOffset + (i + 1) * width).toInt()
                Interval(chromIx, start, Math.min(end, endOffset))
            }
        }
    }

    public fun length(): Int = endOffset - startOffset

    override fun toString(): String = "$chromIx:[$startOffset; $endOffset)"
}

/** An interval spanning multiple chromosomes. */
data class MultiInterval(public override val left: Offset,
                         public override val right: Offset) : Interval

/**
 * A (chromosome, offset) pair.
 */
data class Offset(
        /** Chromosome ID as defined by the B+ index.  */
        public val chromIx: Int,
        /** 0-based genomic offset.  */
        public val offset: Int) : Comparable<Offset> {

    override fun compareTo(other: Offset): Int = ComparisonChain.start()
            .compare(chromIx, other.chromIx)
            .compare(offset, other.offset)
            .result()

    override fun toString(): String = "$chromIx:$offset"
}