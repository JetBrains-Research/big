package org.jetbrains.bio

import com.google.common.base.Stopwatch
import com.google.common.collect.Iterators
import com.google.common.collect.UnmodifiableIterator
import com.google.common.math.IntMath
import com.google.common.math.LongMath
import org.apache.log4j.Logger
import java.io.BufferedReader
import java.math.RoundingMode
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream
import kotlin.reflect.KProperty

/**
 * Various internal helpers.
 *
 * You shouldn't be using them outside of `big`.
 */

internal infix fun Int.divCeiling(other: Int): Int {
    return IntMath.divide(this, other, RoundingMode.CEILING)
}

internal infix fun Long.divCeiling(other: Long): Long {
    return LongMath.divide(this, other, RoundingMode.CEILING)
}

/**
 * Computes the value n such that base^n <= this.
 */
internal infix fun Int.logCeiling(base: Int): Int {
    require(this > 0) { "non-positive number" }
    require(base > 1) { "base must be >1" }

    var rem = this
    var acc = 1
    while (rem > base) {
        rem = rem divCeiling base
        acc++
    }

    return acc
}

internal inline fun <T> Logger.time(message: String, block: () -> T): T {
    debug(message)
    val stopwatch = Stopwatch.createStarted()
    val res = block()
    stopwatch.stop()
    debug("Done in $stopwatch")
    return res
}

/** A function which simply ignores a given [_value]. */
internal fun ignore(_value: Any?) {}

/** A marker function for "impossible" `when` branches. */
inline fun impossible(lazyMsg: () -> String): Nothing = throw IllegalStateException(lazyMsg())

internal operator fun <T> ThreadLocal<T>.getValue(thisRef: Any?, property: KProperty<*>) = get()

internal operator fun <T> ThreadLocal<T>.setValue(thisRef: Any?, property: KProperty<*>, value: T) = set(value)

// XXX calling the 'Iterable<T>#map' leads to boxing.
internal inline fun <R> IntProgression.mapUnboxed(
        crossinline transform: (Int) -> R): Sequence<R> {
    val it = iterator()
    return object : Iterator<R> {
        override fun next() = transform(it.nextInt())

        override fun hasNext() = it.hasNext()
    }.asSequence()
}

internal abstract class CachingIterator<T>(reader: BufferedReader) : UnmodifiableIterator<T>() {
    protected var lines = Iterators.peekingIterator(reader.lines().iterator())
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

/**
 * Lazily groups elements of the sequence into sub-sequences based
 * on the values produced by the key function [f].
 *
 * The user is responsible for consuming the resulting sub-sequences
 * *in order*. Otherwise the implementation might yield unexpected
 * results.
 *
 * No assumptions are made about the monotonicity of [f].
 */
internal fun <T : Any, K> Sequence<T>.groupingBy(f: (T) -> K): Sequence<Pair<K, Sequence<T>>> {
    return Sequence {
        object : Iterator<Pair<K, Sequence<T>>> {
            private var cached: K? = null

            private val it = Iterators.peekingIterator(this@groupingBy.iterator())

            override fun hasNext() = it.hasNext()

            override fun next(): Pair<K, Sequence<T>> {
                val target = f(it.peek())
                assert(cached == null || cached != target) {
                    "group (key: $target) was not consumed fully"
                }

                cached = target
                return target to generateSequence {
                    if (it.hasNext() && f(it.peek()) == cached) {
                        it.next()
                    } else {
                        null
                    }
                }
            }
        }
    }
}

internal fun Path.bufferedReader(vararg options: OpenOption): BufferedReader {
    val inputStream = Files.newInputStream(this, *options).buffered()
    return when (toFile().extension) {
        "gz"  -> GZIPInputStream(inputStream)
        "zip" ->
            // This only works for single-entry ZIP files.
            ZipInputStream(inputStream).apply { getNextEntry() }
        else  -> inputStream
    }.bufferedReader()
}
