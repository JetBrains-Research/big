package org.jetbrains.bio.big

import com.google.common.base.Stopwatch
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import com.google.common.collect.UnmodifiableIterator
import com.google.common.math.IntMath
import org.apache.log4j.Logger
import java.io.BufferedReader
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

fun Sequence<T>.partition<T>(n: Int): Sequence<List<T>> {
    require(n > 1, "n must be >1")
    val that = this
    return object : Sequence<List<T>> {
        override fun iterator(): Iterator<List<T>> {
            return Iterators.partition(that.iterator(), n)
        }
    }
}

inline fun Logger.time<T>(message: String, block: () -> T): T {
    debug(message)
    val stopwatch = Stopwatch.createStarted()
    val res = block()
    stopwatch.stop()
    debug("Done in $stopwatch")
    return res
}

// It's a false positive. Without the ? the code doesn't compile
// at least on M12.
@suppress("base_with_nullable_upper_bound")
abstract class CachingIterator<T>(reader: BufferedReader) : UnmodifiableIterator<T>() {
    protected var lines: PeekingIterator<String> =
            Iterators.peekingIterator(reader.lines().iterator())
    private var cached: T? = null

    override fun hasNext(): Boolean {
        if (cached == null) {
            cached = cache()  // Got some?
        }

        return cached != null
    }

    override fun next(): T? {
        check(hasNext())
        val next = cached
        cached = null
        return next
    }

    protected abstract fun cache(): T?
}