package org.jetbrains.bio

data class ScoredInterval(val start: Int, val end: Int, val score: Float) {
    override fun toString() = "$score@[$start; $end)"
}