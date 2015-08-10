package org.jetbrains.bio.big

import com.google.common.primitives.Floats
import com.google.common.primitives.Ints

data class BigSummary(
        /** An upper bound on the number of (bases) with actual data. */
        public var count: Long = 0L,
        /** Minimum item value. */
        public var minValue: Double = Double.POSITIVE_INFINITY,
        /** Maximum item value. */
        public var maxValue: Double = Double.NEGATIVE_INFINITY,
        /** Sum of values for each base. */
        public var sum: Double = 0.0,
        /** Sum of squares for each base. */
        public var sumSquares: Double = 0.0) {

    fun update(value: Double, intersection: Int, total: Int) {
        val weight = intersection.toDouble() / total
        count += intersection
        sum += value * weight;
        sumSquares += value * value * weight
        minValue = Math.min(minValue, value);
        maxValue = Math.max(maxValue, value);
    }

    companion object {
        fun read(input: SeekableDataInput) = with(input) {
            val count = readLong()
            val minValue = readDouble()
            val maxValue = readDouble()
            val sum = readDouble()
            val sumSquares = readDouble()
            BigSummary(count, minValue, maxValue, sum, sumSquares)
        }
    }
}

data class ZoomLevel(public val reductionLevel: Int,
                     public val dataOffset: Long,
                     public val indexOffset: Long) {
    companion object {
        fun read(input: SeekableDataInput) = with(input) {
            val reductionLevel = readInt()
            val reserved = readInt()
            check(reserved == 0)
            val dataOffset = readLong()
            val indexOffset = readLong()
            ZoomLevel(reductionLevel, dataOffset, indexOffset)
        }
    }
}

fun List<ZoomLevel>.pick(desiredReduction: Int): ZoomLevel? {
    require(desiredReduction >= 0, "desired must be >=0")
    return if (desiredReduction <= 1) {
        null
    } else {
        var acc = Int.MAX_VALUE
        var closest: ZoomLevel? = null
        for (zoomLevel in this) {
            val d = desiredReduction - zoomLevel.reductionLevel
            if (d >= 0 && d < acc) {
                acc = d
                closest = zoomLevel
            }
        }

        closest
    }
}

data class ZoomData(
        /** Chromosome id as defined by B+ tree. */
        val chromIx: Int,
        /** 0-based start offset (inclusive). */
        val startOffset: Int,
        /** 0-based end offset (exclusive). */
        val endOffset: Int,
        /**
         * These are just inlined fields of [BigSummary] downcasted
         * to 4 bytes. Top-notch academic design! */
        val count: Int,
        val minValue: Float,
        val maxValue: Float,
        val sum: Float,
        val sumSquares: Float) {

    val interval: ChromosomeInterval get() = Interval(chromIx, startOffset, endOffset)

    companion object {
        val SIZE: Int = Ints.BYTES * 3 +
                        Ints.BYTES + Floats.BYTES * 4

        fun read(input: OrderedDataInput): ZoomData = with(input) {
            val chromIx = readInt()
            val startOffset = readInt()
            val endOffset = readInt()
            val count = readInt()
            val minValue = readFloat()
            val maxValue = readFloat()
            val sum = readFloat();
            val sumSquares = readFloat();
            return ZoomData(chromIx, startOffset, endOffset, count,
                            minValue, maxValue, sum, sumSquares);
        }
    }
}