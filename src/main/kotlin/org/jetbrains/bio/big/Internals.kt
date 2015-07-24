package org.jetbrains.bio.big

import com.google.common.collect.ComparisonChain
import com.google.common.collect.Ordering
import com.google.common.math.IntMath
import java.math.RoundingMode

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

fun String.trimZeros() = trimEnd { it == '\u0000' }

/**
 * A semi-closed interval.
 */
interface Interval {
    /** Start offset (inclusive).  */
    public val left: Offset
    /** End offset (exclusive).  */
    public val right: Offset

    public fun overlaps(other: Interval): Boolean {
        return !(other.right <= left || other.left >= right)
    }

    /**
     * Returns a union of the two intervals, i.e. an interval which
     * completely covers both of them.
     */
    public fun union(other: Interval): Interval {
        val ord = Ordering.natural<Offset>()
        return MultiInterval(ord.min(left, other.left),
                             ord.max(right, other.right))
    }

    override fun toString(): String = "[$left; $right)"

    companion object {
        fun of(chromIx: Int, startOffset: Int, endOffset: Int): ChromosomeInterval {
            require(startOffset < endOffset, "start must be <end")
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