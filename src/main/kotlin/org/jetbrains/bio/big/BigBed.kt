package org.jetbrains.bio.big

import com.google.common.collect.ComparisonChain
import com.google.common.collect.Lists
import com.google.common.primitives.Ints
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.ArrayList
import java.util.Collections
import kotlin.platform.platformStatic

/**
 * Just like BED only BIGGER.
 */
public class BigBedFile throws(IOException::class) protected constructor(path: Path) :
        BigFile<BedEntry>(path, magic = BigBedFile.MAGIC) {

    /**
     * Truncate empty bins from [ChromosomeInterval.slice].
     */
    private fun ChromosomeInterval.truncatedSlice(bedEntries: List<BedEntry>,
                                                  numBins: Int): Sequence<ChromosomeInterval> {
        return if (bedEntries.isEmpty()) {
            slice(numBins)
        } else {
            val width = length().toDouble() / numBins
            val i = Math.floor(bedEntries.first().start / width).toInt()
            val j = Math.ceil(bedEntries.last().end / width).toInt()
            Interval(chromIx, (width * i).toInt(), (width * j).toInt())
                    .slice(j - i)
        }
    }

    override fun summarizeInternal(query: ChromosomeInterval,
                                   numBins: Int): Sequence<Pair<Int, BigSummary>> {
        val coverage = query(query).aggregate()
        var edge = 0
        return query.truncatedSlice(coverage, numBins).mapIndexed { i, bin ->
            val summary = BigSummary()
            for (j in edge until coverage.size()) {
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

            if (summary.isEmpty()) null else i to summary
        }.filterNotNull()
    }

    override fun queryInternal(dataOffset: Long, dataSize: Long,
                               query: ChromosomeInterval): Sequence<BedEntry> {
        val chrom = chromosomes[query.chromIx]
        return input.with(dataOffset, dataSize, compressed) {
            val chunk = ArrayList<BedEntry>()
            do {
                val chromIx = readInt()
                assert(chromIx == query.chromIx, "interval contains wrong chromosome")
                val startOffset = readInt()
                val endOffset = readInt()
                val sb = StringBuilder()
                while (true) {
                    var ch = readUnsignedByte()
                    if (ch == 0) {
                        break
                    }

                    sb.append(ch.toChar())
                }

                // This was somewhat tricky to get right, please make sure
                // you understand the code before modifying it.
                if (startOffset < query.startOffset || endOffset > query.endOffset) {
                    continue
                } else if (startOffset > query.endOffset) {
                    break
                }

                chunk.add(BedEntry(chrom, startOffset, endOffset, sb.toString()))
            } while (!finished)

            chunk.asSequence()
        }
    }

    companion object {
        /** Magic number used for determining [ByteOrder]. */
        val MAGIC: Int = 0x8789F2EB.toInt()

        throws(IOException::class)
        public platformStatic fun read(path: Path): BigBedFile = BigBedFile(path)

        /**
         * Creates a BigBED file from given entries.
         *
         * @param bedEntries entries to write and index.
         * @param chromSizesPath path to the TSV file with chromosome
         *                       names and sizes.
         * @param outputPath BigBED file path.
         * @param itemsPerSlot number of items to store in a single
         *                     R+ tree index node. Defaults to `1024`.
         * @param zoomLevelCount number of zoom levels to pre-compute.
         *                       Defaults to `8`.
         * @param compressed compress BigBED data sections with gzip.
         *                   Defaults to `false`.
         * @param order byte order used, see [java.nio.ByteOrder].
         * @throws IOException if any of the read or write operations failed.
         */
        throws(IOException::class)
        public platformStatic fun write(bedEntries: Iterable<BedEntry>,
                                        chromSizesPath: Path,
                                        outputPath: Path,
                                        itemsPerSlot: Int = 1024,
                                        zoomLevelCount: Int = 8,
                                        compressed: Boolean = true,
                                        order: ByteOrder = ByteOrder.nativeOrder()) {
            SeekableDataOutput.of(outputPath, order).use { output ->
                output.skipBytes(0, BigFile.Header.BYTES)
                output.skipBytes(0, ZoomLevel.BYTES * zoomLevelCount)
                val totalSummaryOffset = output.tell()
                output.skipBytes(0, BigSummary.BYTES)

                val unsortedChromosomes = chromSizesPath.chromosomes()
                val chromTreeOffset = output.tell()
                BPlusTree.write(output, unsortedChromosomes)

                val unzoomedDataOffset = output.tell()
                val resolver = unsortedChromosomes.map { it.key to it.id }.toMap()
                val leaves = Lists.newArrayList<RTreeIndexLeaf>()
                var uncompressBufSize = 0
                bedEntries.groupBy { it.chrom }.forEach { entry ->
                    val (name, items) = entry
                    Collections.sort(items) { e1, e2 -> Ints.compare(e1.start, e2.start) }

                    val chromIx = resolver[name]!!
                    for (i in 0 until items.size() step itemsPerSlot) {
                        val dataOffset = output.tell()
                        val start = items[i].start
                        var end = 0
                        val current = output.with(compressed) {
                            val slotSize = Math.min(items.size() - i, itemsPerSlot)
                            for (j in 0 until slotSize) {
                                val item = items[i + j]
                                writeInt(chromIx)
                                writeInt(item.start)
                                writeInt(item.end)
                                writeBytes("${item.name},${item.score},${item.strand},${item.rest}")
                                writeByte(0)  // null-terminated.

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

                val header = BigFile.Header(
                        output.order, MAGIC, zoomLevelCount = zoomLevelCount,
                        chromTreeOffset = chromTreeOffset,
                        unzoomedDataOffset = unzoomedDataOffset,
                        unzoomedIndexOffset = unzoomedIndexOffset,
                        fieldCount = 3, definedFieldCount = 3,
                        totalSummaryOffset = totalSummaryOffset,
                        uncompressBufSize = if (compressed) uncompressBufSize else 0)
                header.write(output)
            }

            BigFile.Post.zoom(outputPath)
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
private fun Sequence<BedEntry>.aggregate(): List<BedEntry> {
    val events = flatMap {
        listOf(AggregationEvent(it.start, START, it),
               AggregationEvent(it.end, END, it)).asSequence()
    }.toArrayList()

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
            event.type == END || i == events.size() - 1 -> {
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