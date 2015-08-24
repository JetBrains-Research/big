package org.jetbrains.bio.big

import java.io.BufferedReader
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.Path
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream

/**
 * Various test helpers.
 *
 * You shouldn't be using them outside of `big`.
 */

// XXX use sparingly, because the optimizer fails to translate
// this into a pure-Java forloop. See KT-8901.
fun Int.until(other: Int) = this..other - 1

fun Path.bufferedReader(vararg options: OpenOption): BufferedReader {
    val inputStream = Files.newInputStream(this, *options).buffered()
    return when (toFile().extension) {
        "gz"  -> GZIPInputStream(inputStream)
        "zip" -> ZipInputStream(inputStream)
        else  -> inputStream
    }.bufferedReader()
}

/** Fetches chromosome sizes from a UCSC provided TSV file. */
fun Path.chromosomes(): List<Pair<String, Int>> {
    return bufferedReader().lineSequence().map { line ->
        val chunks = line.split('\t', limit = 3)
        chunks[0] to chunks[1].toInt()
    }.toList()
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