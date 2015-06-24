package org.jbb.big

import com.google.common.collect.ComparisonChain
import java.util.*
import kotlin.platform.platformStatic

/**
 * Chromosome R-tree external node format
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
data class RTreeIndexLeaf(public val interval: RTreeInterval,
                          public val dataOffset: Long,
                          public val dataSize: Long)

/**
 * Internal node of the chromosome R-tree.
 *
 * @author Sergey Zherevchuk
 * @since 13/03/15
 */
data class RTreeIndexNode(public val interval: RTreeInterval, public val dataOffset: Long)

/**
 * A semi-closed interval.
 *
 * @author Sergei Lebedev
 * @since 16/03/15
 */
data class RTreeInterval(
        /** Start offset (inclusive).  */
        public val left: RTreeOffset,
        /** End offset (exclusive).  */
        public val right: RTreeOffset) {

    public fun overlaps(other: RTreeInterval): Boolean {
        return !(other.right <= left || other.left >= right)
    }

    override fun toString(): String = "[$left; $right)"

    companion object {
        platformStatic fun of(chromIx: Int, startOffset: Int, endOffset: Int): RTreeInterval {
            return of(chromIx, startOffset, chromIx, endOffset)
        }

        platformStatic fun of(startChromIx: Int, startOffset: Int,
                              endChromIx: Int, endOffset: Int): RTreeInterval {
            return RTreeInterval(RTreeOffset(startChromIx, startOffset),
                                 RTreeOffset(endChromIx, endOffset))
        }
    }
}

/**
 * A (chromosome, offset) pair.
 *
 * @author Sergei Lebedev
 * @since 16/03/15
 */
data class RTreeOffset(
        /** Chromosome ID as defined by the B+ index.  */
        public val chromIx: Int,
        /** 0-based genomic offset.  */
        public val offset: Int) : Comparable<RTreeOffset> {

    override fun compareTo(other: RTreeOffset): Int = ComparisonChain.start()
            .compare(chromIx, other.chromIx)
            .compare(offset, other.offset)
            .result()

    override fun toString(): String = "$chromIx:$offset"
}

