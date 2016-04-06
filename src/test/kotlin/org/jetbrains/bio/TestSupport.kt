package org.jetbrains.bio

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

internal object Examples {
    @JvmStatic operator fun get(name: String): Path {
        val url = Examples.javaClass.classLoader.getResource(name)
                  ?: throw IllegalStateException("resource not found")

        return Paths.get(url.toURI()).toFile().toPath()
    }
}

/** Fetches chromosome sizes from a UCSC provided TSV file. */
internal fun Path.chromosomes(): List<Pair<String, Int>> {
    return bufferedReader().lineSequence().map { line ->
        val chunks = line.split('\t', limit = 3)
        chunks[0] to chunks[1].toInt()
    }.toList()
}

internal inline fun withTempFile(prefix: String, suffix: String,
                                 block: (Path) -> Unit) {
    val path = Files.createTempFile(prefix, suffix)
    try {
        block(path)
    } finally {
        try {
            Files.delete(path)
        } catch (e: IOException) {
            // Mmaped buffer not yet garbage collected. Leave it to the VM.
            path.toFile().deleteOnExit()
        }
    }
}
