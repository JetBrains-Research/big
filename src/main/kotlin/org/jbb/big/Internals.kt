package org.jbb.big

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors
import kotlin.platform.platformStatic

/**
 * Various internal helpers.
 *
 * You shouldn't be using them outside of `big`.
 *
 * @author Sergei Lebedev
 * @since 24/06/15
 */
object Internals {
    /**
     * Reads chromosome sizes from a tab-delimited two-column file.
     */
    throws(IOException::class)
    platformStatic fun readChromosomeSizes(path: Path): Map<String, Int> {
        return Files.lines(path).map { it.split('\t') }
                .collect(Collectors.toMap({ it[0] }, { it[1].toInt() }))
    }
}