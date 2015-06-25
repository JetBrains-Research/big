package org.jbb.big

import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.platform.platformStatic

/**
 * A summary of a single BED file.
 *
 * Used in [RTreeIndex] construction.
 *
 * @author Sergei Lebedev
 * @since 24/06/15
 */
class BedSummary(private val chromSizes: Map<String, Int>) {
    private val entries: MutableMap<String, Entry> = LinkedHashMap()

    /** Total length of all the items in a BED file. */
    public val baseCount: Int get() = entries.values().map { it.baseCount }.sum()
    /** Total number of items in a BED file. */
    public val itemCount: Int get() = entries.values().map { it.itemCount }.sum()

    private fun add(name: String, start: Int, end: Int) {
        check(start < end) { "start $start before end $end" }
        var size = checkNotNull(chromSizes[name]) {
            "$name not found in chromosome sizes file"
        }

        check(end <= size) { "end coordinate $end bigger than size $size" }

        val entry = entries.getOrPut(name, { Entry() })
        entry.baseCount += end - start
        entry.itemCount++
    }

    // XXX Summary stats include chromosome size and ID. We must get rid of this.
    public fun toList(): List<bbiChromUsage> = entries.keySet().mapIndexed { id, name ->
        val usage = bbiChromUsage(name, id, chromSizes[name]!!)
        usage.itemCount = entries[name]!!.itemCount
        usage
    }.toList()

    private data class Entry(var baseCount: Int = 0, var itemCount: Int = 0)

    companion object {
        /**
         * Computes [BedSummary] from a given BED file.
         *
         * Path to chromosome sizes file is currently required, because of
         * the way the code uses [bbiChromUsage]. In theory, we can compute
         * "effective" chromosome sizes from the BED file and use these
         * instead of "real" ones.
         */
        platformStatic fun of(bedPath: Path, chromSizesPath: Path): BedSummary {
            val chromSizes = readChromosomeSizes(chromSizesPath);
            val summary = BedSummary(chromSizes)
            for (item in BedFile.read(bedPath)) {
                summary.add(item.name, item.start, item.end)
            }

            return summary
        }
    }
}