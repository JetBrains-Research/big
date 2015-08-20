package org.jetbrains.bio.big

import com.google.common.base.Stopwatch
import com.google.common.collect.Iterators
import com.google.common.math.IntMath
import org.apache.log4j.Logger
import java.io.BufferedReader
import java.math.RoundingMode
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream

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

// XXX use sparingly, because the optimizer fails to translate
// this into a pure-Java forloop. See KT-8901.
fun Int.until(other: Int) = this..other - 1

// XXX calling the 'Iterable<T>#map' leads to boxing.
private class TransformingIntIterator<R>(private val it: IntIterator,
                                         private val transform: (Int) -> R) :
        Iterator<R> {

    override fun next(): R = transform(it.nextInt())

    override fun hasNext(): Boolean = it.hasNext()
}

fun IntRange.mapUnboxed<R>(transform: (Int) -> R): Sequence<R> {
    return TransformingIntIterator(iterator(), transform).asSequence()
}

fun IntProgression.mapUnboxed<R>(transform: (Int) -> R): Sequence<R> {
    return TransformingIntIterator(iterator(), transform).asSequence()
}

fun String.trimZeros() = trimEnd { it == '\u0000' }

fun Path.bufferedReader(vararg options: OpenOption): BufferedReader {
    val inputStream = Files.newInputStream(this, *options).buffered()
    return when (toFile().extension) {
        "gz"  -> GZIPInputStream(inputStream)
        "zip" -> ZipInputStream(inputStream)
        else  -> inputStream
    }.bufferedReader()
}

inline fun withTempFile(prefix: String, suffix: String,
                        block: (Path) -> Unit) {
    val path = Files.createTempFile(prefix, suffix)
    try {
        block(path)
    } finally {
        Files.delete(path)
    }
}

/** Fetches chromosome sizes from a UCSC provided TSV file. */
fun Path.chromosomes(): List<BPlusLeaf> {
    return bufferedReader().lineSequence().mapIndexed { i, line ->
        val chunks = line.split('\t', limit = 3)
        BPlusLeaf(chunks[0], i, chunks[1].toInt())
    }.toList()
}

fun Sequence<T>.partition<T>(n: Int): Sequence<List<T>> {
    require(n > 0, "n must be >0")
    val that = this
    return object : Sequence<List<T>> {
        override fun iterator(): Iterator<List<T>> {
            return Iterators.partition(that.iterator(), n)
        }
    }
}

inline fun Logger.time(message: String, block: () -> Unit) {
    debug(message)
    val stopwatch = Stopwatch.createStarted()
    block()
    stopwatch.stop()
    debug("Done in $stopwatch")
}