package org.jetbrains.bio.big

data class BigSummary(
        /** An upper bound on the number of (bases) with actual data. */
        public val count: Long,
        /** Minimum item value. */
        public val minValue: Double,
        /** Maximum item value. */
        public val maxValue: Double,
        /** Sum of values for each base. */
        public val sum: Double,
        /** Sum of squares for each base. */
        public val sumSquares: Double) {
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

class IntervalStatistics(private val length: Int) {
    private var count = 0L
    private var min = Double.POSITIVE_INFINITY
    private var max = Double.NEGATIVE_INFINITY
    private var sum = 0.0
    private var sumSquares = 0.0

    fun add(value: Double, intersection: Int) {
        val weight = intersection.toDouble() / length
        count += intersection;
        sum += value * weight;
        sumSquares += value * value * weight
        min = Math.min(min, value);
        max = Math.max(max, value);
    }

    val summary: BigSummary get() {
        return BigSummary(count = count,
                          minValue = min, maxValue = max,
                          sum = sum,
                          sumSquares = sumSquares)
    }
}