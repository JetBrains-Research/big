package org.jbb.big

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