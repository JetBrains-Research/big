package org.jetbrains.bio.big

import com.google.common.collect.ComparisonChain
import com.google.common.collect.Lists
import com.google.common.primitives.Ints
import java.io.IOException
import java.nio.file.Path
import java.util.ArrayList
import java.util.Collections
import kotlin.platform.platformStatic

/**
 * Just like BED only BIGGER.
 */
public class BigBedFile throws(IOException::class) protected constructor(path: Path) :
        BigFile<BedEntry>(path, magic = BigBedFile.MAGIC) {

    override fun summarizeInternal(query: ChromosomeInterval, numBins: Int): List<BigSummary> {
        val bedEntries = query(query).aggregate()
        var edge = 0
        return query.slice(numBins).map { bin ->
            var count = 0L
            var min = Double.POSITIVE_INFINITY
            var max = Double.NEGATIVE_INFINITY
            var sum = 0.0
            var sumSquares = 0.0

            for (j in edge until bedEntries.size()) {
                val bedEntry = bedEntries[j]
                if (bedEntry.end <= bin.startOffset) {
                    edge = j + 1
                    continue
                } else if (bedEntry.start > bin.endOffset) {
                    break
                }

                val interval = Interval.of(query.chromIx, bedEntry.start, bedEntry.end)
                if (interval intersects bin) {
                    val intersection = interval intersection bin
                    assert(intersection.length() > 0)
                    val value = bedEntry.score.toDouble()
                    val weight = intersection.length().toDouble() / interval.length()
                    count += intersection.length();
                    sum += value * weight;
                    sumSquares += value * value * weight
                    min = Math.min(min, value);
                    max = Math.max(max, value);
                }
            }

            BigSummary(count = count, minValue = min, maxValue = max,
                       sum = sum, sumSquares = sumSquares)
        }.toList()
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
        private val MAGIC: Int = 0x8789F2EB.toInt()

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
         * @param compressed compress BigBED data sections with gzip.
         *                   Defaults to `false`.
         * @throws IOException if any of the read or write operations failed.
         */
        throws(IOException::class)
        public platformStatic fun write(bedEntries: Iterable<BedEntry>,
                                        chromSizesPath: Path,
                                        outputPath: Path,
                                        itemsPerSlot: Int = 1024,
                                        compressed: Boolean = true) {
            SeekableDataOutput.of(outputPath).use { output ->
                output.skipBytes(0, BigFile.Header.BYTES)

                val unsortedChromosomes = chromSizesPath.bufferedReader()
                        .lineSequence().mapIndexed { i, line ->
                    val chunks = line.split('\t', limit = 3)
                    BPlusLeaf(chunks[0], i, chunks[1].toInt())
                }.toList()

                val chromTreeOffset = output.tell()
                BPlusTree.write(output, unsortedChromosomes)

                // XXX move to 'BedFile'?
                val unzoomedDataOffset = output.tell()
                val resolver = unsortedChromosomes.map { it.key to it.id }.toMap()
                val leaves = Lists.newArrayList<RTreeIndexLeaf>()
                var uncompressBufSize = 0
                bedEntries.groupBy { it.chrom }.forEach { entry ->
                    val (name, items) = entry
                    Collections.sort(items) { e1, e2 -> Ints.compare(e1.start, e2.start) }

                    val chromId = resolver[name]!!
                    for (i in 0 until items.size() step itemsPerSlot) {
                        val dataOffset = output.tell()
                        val start = items[i].start
                        var end = 0
                        val current = output.with(compressed) {
                            val slotSize = Math.min(items.size() - i, itemsPerSlot)
                            for (j in 0 until slotSize) {
                                val item = items[i + j]
                                writeInt(chromId)
                                writeInt(item.start)
                                writeInt(item.end)
                                writeBytes("${item.name},${item.score},${item.strand},${item.rest}")
                                writeByte(0)  // null-terminated.

                                end = Math.max(end, item.end)
                            }
                        }

                        leaves.add(RTreeIndexLeaf(
                                Interval.of(chromId, start, end),
                                dataOffset, output.tell() - dataOffset))
                        uncompressBufSize = Math.max(uncompressBufSize, current)
                    }
                }

                val unzoomedIndexOffset = output.tell()
                RTreeIndex.write(output, leaves, itemsPerSlot = itemsPerSlot)

                val header = BigFile.Header(
                        output.order,
                        chromTreeOffset = chromTreeOffset,
                        unzoomedDataOffset = unzoomedDataOffset,
                        unzoomedIndexOffset = unzoomedIndexOffset,
                        fieldCount = 3, definedFieldCount = 3,
                        uncompressBufSize = if (compressed) uncompressBufSize else 0)
                header.write(output, MAGIC)
            }
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
fun Sequence<BedEntry>.aggregate(): List<BedEntry> {
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