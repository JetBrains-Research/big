package org.jbb.big

import com.google.common.collect.ComparisonChain
import com.google.common.collect.Ordering
import com.google.common.math.IntMath
import java.math.RoundingMode
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

/**
 * Various internal helpers.
 *
 * You shouldn't be using them outside of `big`.
 *
 * @author Sergei Lebedev
 * @since 24/06/15
 */

/**
 * Reads chromosome sizes from a tab-delimited two-column file.
 */
fun readChromosomeSizes(path: Path): Map<String, Int> = Files.lines(path)
        .map { it.split('\t') }
        .collect(Collectors.toMap({ it[0] }, { it[1].toInt() }))


fun Int.divCeiling(other: Int) = IntMath.divide(this, other, RoundingMode.CEILING)

// Remove once KT-8248 is done.
fun Int.pow(other: Int) = IntMath.pow(this, other)

/**
 * Computes the value n such that base^n <= this.
 */
fun Int.logCeiling(base: Int): Int {
    require(this > 0, "non-positive number")
    require(base > 0, "non-positive base")

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

fun String.trimZeros() = trimEnd { it == '\u0000' }

// Remove once KT-8267 is done.
fun List<T>.foldRight<T>(accumulator: (T, T) -> T): T {
    return subList(1, size()).foldRight(this[0], accumulator)
}

/**
 * A semi-closed interval.
 *
 * @author Sergei Lebedev
 * @since 16/03/15
 */
interface  Interval {
    /** Start offset (inclusive).  */
    public val left: Offset
    /** End offset (exclusive).  */
    public val right: Offset

    public fun overlaps(other: Interval): Boolean {
        return !(other.right <= left || other.left >= right)
    }

    /**
     * Returns a union of the two intervals.
     */
    public fun union(other: Interval): Interval {
        val ord = Ordering.natural<Offset>()
        return MultiInterval(ord.min(left, other.left),
                             ord.max(right, other.right))
    }

    override fun toString(): String = "[$left; $right)"

    companion object {
        fun of(chromIx: Int, startOffset: Int, endOffset: Int): ChromosomeInterval {
            return ChromosomeInterval(chromIx, startOffset, endOffset)
        }

        fun of(startChromIx: Int, startOffset: Int,
               endChromIx: Int, endOffset: Int): Interval {
            return if (startChromIx == endChromIx) {
                of(startChromIx, startOffset, endOffset)
            } else {
                MultiInterval(Offset(startChromIx, startOffset),
                              Offset(endChromIx, endOffset))
            }

        }
    }
}

/** An interval on a chromosome. */
data class ChromosomeInterval(public val chromIx: Int,
                              public val startOffset: Int,
                              public val endOffset: Int) : Interval {
    override val left: Offset get() = Offset(chromIx, startOffset)
    override val right: Offset get() = Offset(chromIx, endOffset)

    override fun toString(): String = "$chromIx:[$startOffset; $endOffset)"
}

/** An interval spanning multiple chromosomes. */
data class MultiInterval(public override val left: Offset,
                         public override val right: Offset) : Interval

/**
 * A (chromosome, offset) pair.
 *
 * @author Sergei Lebedev
 * @since 16/03/15
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