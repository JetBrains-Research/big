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