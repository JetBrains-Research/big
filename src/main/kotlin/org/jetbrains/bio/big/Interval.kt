package org.jetbrains.bio.big

import com.google.common.collect.ComparisonChain
import com.google.common.collect.Ordering

/**
 * A semi-closed interval.
 */
interface Interval {
    /** Start offset (inclusive).  */
    public val left: Offset
    /** End offset (exclusive).  */
    public val right: Offset

    /**
     * Returns `true` if a given interval intersects this interval
     * and `false` otherwise.
     */
    public fun intersects(other: Interval): Boolean

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

    fun write(output: OrderedDataOutput): Unit

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

    override fun intersects(other: Interval): Boolean = when (other) {
        is ChromosomeInterval -> {
            // Specialized, because allocating offsets in '#left' and
            // '#right' only for checking for intersection is expensive.
            if (chromIx == other.chromIx) {
                !(other.endOffset <= startOffset || other.startOffset >= endOffset)
            } else {
                false
            }
        }
        else -> other.intersects(this)
    }

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
            (0..n - 1).mapUnboxed { i ->
                val start = Math.round(startOffset + i * width).toInt()
                val end = Math.round(startOffset + (i + 1) * width).toInt()
                Interval(chromIx, start, Math.min(end, endOffset))
            }
        }
    }

    public fun length(): Int = endOffset - startOffset

    override fun write(output: OrderedDataOutput) = with(output) {
        writeInt(chromIx)
        writeInt(startOffset)
        writeInt(chromIx)
        writeInt(endOffset)
    }

    override fun toString(): String = "$chromIx:[$startOffset; $endOffset)"
}

/** An interval spanning multiple chromosomes. */
data class MultiInterval(public override val left: Offset,
                         public override val right: Offset) : Interval {

    override fun intersects(other: Interval): Boolean {
        return !(other.right <= left || other.left >= right)
    }

    override fun write(output: OrderedDataOutput) = with(output) {
        writeInt(left.chromIx)
        writeInt(left.offset)
        writeInt(right.chromIx)
        writeInt(right.offset)
    }
}

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
