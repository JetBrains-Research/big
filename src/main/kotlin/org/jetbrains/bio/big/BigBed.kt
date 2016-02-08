package org.jetbrains.bio.big

import com.google.common.collect.ComparisonChain
import com.google.common.collect.Lists
import org.jetbrains.bio.CountingDataOutput
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.*

/**
 * Just like BED only BIGGER.
 */
class BigBedFile @Throws(IOException::class) protected constructor(path: Path) :
        BigFile<BedEntry>(path, magic = BigBedFile.MAGIC) {

    override fun summarizeInternal(query: ChromosomeInterval,
                                   numBins: Int): Sequence<IndexedValue<BigSummary>> {
        val coverage = query(query, overlaps = true).aggregate()
        var edge = 0
        return query.slice(numBins).mapIndexed { i, bin ->
            val summary = BigSummary()
            for (j in edge..coverage.size - 1) {
                val bedEntry = coverage[j]
                if (bedEntry.end <= bin.startOffset) {
                    edge = j + 1
                    continue;
                } else if (bedEntry.start > bin.endOffset) {
                    break
                }

                val interval = Interval(query.chromIx, bedEntry.start, bedEntry.end)
                if (interval intersects bin) {
                    summary.update(bedEntry.score.toDouble(),
                                   (interval intersection bin).length(),
                                   interval.length())
                }
            }

            if (summary.isEmpty()) null else IndexedValue(i, summary)
        }.filterNotNull()
    }

    /**
     * Returns `true` if a given entry is consistent with the query.
     * That is
     *   it either intersects the query (and overlaps is `true`)
     *   or it is completely contained in the query.
     */
    private fun ChromosomeInterval.contains(startOffset: Int, endOffset: Int,
                                            overlaps: Boolean): Boolean {
        val interval = Interval(chromIx, startOffset, endOffset)
        return (overlaps && interval intersects this) || interval in this
    }

    override fun queryInternal(dataOffset: Long, dataSize: Long,
                               query: ChromosomeInterval,
                               overlaps: Boolean): Sequence<BedEntry> {
        val chrom = chromosomes[query.chromIx]
        return input.with(dataOffset, dataSize, compressed) {
            val chunk = ArrayList<BedEntry>()
            do {
                val chromIx = getInt()
                assert(chromIx == query.chromIx) { "interval contains wrong chromosome" }
                val startOffset = getInt()
                val endOffset = getInt()
                val rest = getCString()
                if (query.contains(startOffset, endOffset, overlaps)) {
                    chunk.add(BedEntry(chrom, startOffset, endOffset, rest))
                }
            } while (hasRemaining())

            chunk.asSequence()
        }
    }

    companion object {
        /** Magic number used for determining [ByteOrder]. */
        internal val MAGIC: Int = 0x8789F2EB.toInt()

        @Throws(IOException::class)
        @JvmStatic fun read(path: Path) = BigBedFile(path)

        /**
         * Creates a BigBED file from given entries.
         *
         * @param bedEntries entries to write and index.
         * @param chromSizes chromosome names and sizes, e.g.
         *                   `("chrX", 59373566)`.
         * @param outputPath BigBED file path.
         * @param itemsPerSlot number of items to store in a single
         *                     R+ tree index node. Defaults to `1024`.
         * @param zoomLevelCount number of zoom levels to pre-compute.
         *                       Defaults to `8`.
         * @param compressed compress BigBED data sections with gzip.
         *                   Defaults to `true`.
         * @param order byte order used, see [java.nio.ByteOrder].
         * @@throws IOException if any of the read or write operations failed.
         */
        @Throws(IOException::class)
        @JvmStatic fun write(bedEntries: Iterable<BedEntry>,
                             chromSizes: Iterable<Pair<String, Int>>,
                             outputPath: Path,
                             itemsPerSlot: Int = 1024,
                             zoomLevelCount: Int = 8,
                             compressed: Boolean = true,
                             order: ByteOrder = ByteOrder.nativeOrder()) {
            val groupedEntries = bedEntries.groupBy { it.chrom }
            val header = CountingDataOutput.of(outputPath, order).use { output ->
                output.skipBytes(BigFile.Header.BYTES)
                output.skipBytes(ZoomLevel.BYTES * zoomLevelCount)
                val totalSummaryOffset = output.tell()
                output.skipBytes(BigSummary.BYTES)

                val unsortedChromosomes = chromSizes.mapIndexed { i, p ->
                    BPlusLeaf(p.first, i, p.second)
                }.filter { it.key in groupedEntries }
                val chromTreeOffset = output.tell()
                BPlusTree.write(output, unsortedChromosomes)

                val unzoomedDataOffset = output.tell()
                val resolver = unsortedChromosomes.map { it.key to it.id }.toMap()
                val leaves = Lists.newArrayList<RTreeIndexLeaf>()
                var uncompressBufSize = 0
                for ((name, items) in groupedEntries) {
                    Collections.sort(items)

                    val chromIx = resolver[name]!!
                    for (i in 0..items.size - 1 step itemsPerSlot) {
                        val dataOffset = output.tell()
                        val start = items[i].start
                        var end = 0
                        val current = output.with(compressed) {
                            for (j in 0..Math.min(items.size - i, itemsPerSlot) - 1) {
                                val item = items[i + j]
                                writeInt(chromIx)
                                writeInt(item.start)
                                writeInt(item.end)
                                writeCString("${item.name},${item.score},${item.strand},${item.rest}")

                                end = Math.max(end, item.end)
                            }
                        }

                        leaves.add(RTreeIndexLeaf(
                                Interval(chromIx, start, end),
                                dataOffset, output.tell() - dataOffset))
                        uncompressBufSize = Math.max(uncompressBufSize, current)
                    }
                }

                val unzoomedIndexOffset = output.tell()
                RTreeIndex.write(output, leaves, itemsPerSlot = itemsPerSlot)

                BigFile.Header(
                        output.order, MAGIC, zoomLevelCount = zoomLevelCount,
                        chromTreeOffset = chromTreeOffset,
                        unzoomedDataOffset = unzoomedDataOffset,
                        unzoomedIndexOffset = unzoomedIndexOffset,
                        fieldCount = 3, definedFieldCount = 3,
                        totalSummaryOffset = totalSummaryOffset,
                        uncompressBufSize = if (compressed) uncompressBufSize else 0)
            }

            CountingDataOutput.of(outputPath, order).use { header.write(it) }

            var sum = 0L
            var count = 0
            for (section in groupedEntries.values.flatten()) {
                sum += section.end - section.start
                count++
            }

            // XXX this can be precomputed with a single pass along with the
            // chromosomes used in the source BED.
            val initial = Math.max((sum.toDouble() / count).toInt(), 1) * 8
            BigFile.Post.zoom(outputPath, itemsPerSlot, initial = initial)
            BigFile.Post.totalSummary(outputPath)
        }
    }
}

private class AggregationEvent(val offset: Int, val type: Int,
                               val item: BedEntry) : Comparable<AggregationEvent> {

    override fun toString() = "${if (type == END) "END" else "START"}@$offset"

    override fun compareTo(other: AggregationEvent): Int = ComparisonChain.start()
            .compare(offset, other.offset)
            .compare(type, other.type)
            .result()
}

private val END = 0    // must be before start.
private val START = 1

/** Computes intervals of uniform coverage. */
internal fun Sequence<BedEntry>.aggregate(): List<BedEntry> {
    val events = flatMap {
        listOf(AggregationEvent(it.start, START, it),
               AggregationEvent(it.end, END, it)).asSequence()
    }.toMutableList()

    Collections.sort(events)

    var current = 0
    var left = 0
    val res = ArrayList<BedEntry>()
    events.forEachIndexed { i, event ->
        when {
            event.type == START -> {
                if (current == 0) {
                    left = event.offset
                }

                current += 1
            }
            event.type == END || i == events.size - 1 -> {
                assert(event.offset >= left)
                // Produce a single aggregate for duplicate intervals.
                // For ease of use we abuse the semantics of the
                // '#score' field in 'BedEntry'.
                if (event.offset > left) {
                    val item = event.item
                    res.add(BedEntry(item.chrom, left, event.offset,
                                     item.name, current.toShort(), item.strand,
                                     item.rest))

                    left = event.offset
                }

                current -= 1
            }
        }
    }

    return res
}