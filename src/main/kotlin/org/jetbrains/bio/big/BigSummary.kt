package org.jetbrains.bio.big

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