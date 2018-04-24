package org.jetbrains.bio.big

import org.jetbrains.bio.*
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.*

/**
 * Bigger brother of the good-old WIG format.
 */
class BigWigFile private constructor(
        path: String,
        buffFactory: RomBufferFactory,
        magic: Int,
        prefetch: Boolean,
        cancelledChecker: (() -> Unit)?
) : BigFile<WigSection>(path, buffFactory, magic, prefetch, cancelledChecker) {

    override fun summarizeInternal(
            input: RomBuffer,
            query: ChromosomeInterval,
            numBins: Int,
            cancelledChecker: (() -> Unit)?
    ): Sequence<IndexedValue<BigSummary>> {
        val wigItems = query(input, query, overlaps = true, cancelledChecker = cancelledChecker)
                .flatMap { it.query() }
                .toList()
        var edge = 0
        return query.slice(numBins).mapIndexed { i, bin ->
            val summary = BigSummary()
            for (j in edge until wigItems.size) {
                val wigItem = wigItems[j]
                if (wigItem.end <= bin.startOffset) {
                    edge = j + 1
                    continue
                } else if (wigItem.start > bin.endOffset) {
                    break
                }

                val interval = Interval(query.chromIx, wigItem.start, wigItem.end)
                if (interval intersects bin) {
                    summary.update(wigItem.score.toDouble(),
                            interval.intersectionLength(bin))
                }
            }

            if (summary.isEmpty()) null else IndexedValue(i, summary)
        }.filterNotNull()
    }

    /**
     * Returns `true` if a given item is consistent with the query.
     * That is
     *   it either intersects the query (and overlaps is `true`)
     *   or it is completely contained in the query.
     */
    private fun ChromosomeInterval.contains(startOffset: Int,
                                            endOffset: Int,
                                            overlaps: Boolean): Boolean {
        val interval = Interval(chromIx, startOffset, endOffset)
        return (overlaps && interval intersects this) || interval in this
    }

    override fun queryInternal(decompressedBlock: RomBuffer,
                               query: ChromosomeInterval,
                               overlaps: Boolean): Sequence<WigSection> {
        val chrom = chromosomes[query.chromIx]

        return sequenceOf(with(decompressedBlock) {
            val chromIx = readInt()
            check(chromIx == query.chromIx, {
                "interval contains wrong chromosome $chromIx, expected ${query.chromIx}, file: $path"
            })
            val start = readInt()
            readInt()   // end.
            val step = readInt()
            val span = readInt()
            val type = readUnsignedByte()
            readByte()  // reserved.
            val count = readUnsignedShort()

            val types = WigSection.Type.values()
            check(type >= 1 && type <= types.size)
            when (types[type - 1]) {
                WigSection.Type.BED_GRAPH -> {
                    val section = BedGraphSection(chrom)
                    var match = false
                    for (i in 0 until count) {
                        val startOffset = readInt()
                        val endOffset = readInt()
                        val value = readFloat()
                        if (query.contains(startOffset, endOffset, overlaps)) {
                            section[startOffset, endOffset] = value
                            match = true
                        } else {
                            if (match) {
                                break
                            }
                        }
                    }

                    section
                }
                WigSection.Type.VARIABLE_STEP -> {
                    val section = VariableStepSection(chrom, span)
                    var match = false
                    for (i in 0 until count) {
                        val pos = readInt()
                        val value = readFloat()
                        if (query.contains(pos, pos + span, overlaps)) {
                            section[pos] = value
                            match = true
                        } else {
                            if (match) {
                                break
                            }
                        }
                    }

                    section
                }
                WigSection.Type.FIXED_STEP -> {
                    // Realign section start to the first interval consistent
                    // with the query. See '#contains' above for the definition
                    // of "consistency".

                    // Example:
                    //     |------------------|  query
                    //   |...|...|...|           section
                    //     ^^
                    //   margin = 2
                    val margin = query.startOffset % step
                    val shift = when {
                        margin == 0 -> 0             // perfectly aligned.
                        overlaps -> -margin        // align to the left.
                        else -> step - margin  // align to the right.
                    }

                    val realignedStart = Math.max(start, query.startOffset + shift)
                    val section = FixedStepSection(chrom, realignedStart, step, span)
                    var match = false
                    for (i in 0 until count) {
                        val pos = start + i * step
                        val value = readFloat()
                        if (query.contains(pos, pos + span, overlaps)) {
                            section.add(value)
                            match = true
                        } else {
                            if (match) {
                                break
                            }
                        }
                    }

                    section
                }
            }
        })
    }

    companion object {
        /** Magic number used for determining [ByteOrder]. */
        internal val MAGIC = 0x888FFC26.toInt()


        @Throws(IOException::class)
        @JvmStatic
        fun read(path: Path, cancelledChecker: (() -> Unit)? = null) = read(path.toString(), cancelledChecker = cancelledChecker)

        @Throws(IOException::class)
        @JvmStatic
        fun read(src: String, prefetch: Boolean = false,
                 cancelledChecker: (() -> Unit)? = null,
                 factoryProvider: RomBufferFactoryProvider = defaultFactory()
        ): BigWigFile {
            val factory = factoryProvider(src, ByteOrder.LITTLE_ENDIAN)
            val byteOrder = getByteOrder(src, MAGIC, factory)
            factory.order = byteOrder

            return BigWigFile(src, factory, MAGIC, prefetch, cancelledChecker)
        }

        private class WigSectionSummary {
            val chromosomes = HashSet<String>()
            var count = 0
            var sum = 0L

            /** Makes sure the sections are sorted by offset. */
            private var edge = 0
            /** Makes sure the sections are sorted by chromosome. */
            private var previous = ""

            operator fun invoke(section: WigSection) {
                val switch = section.chrom !in chromosomes
                require(section.chrom == previous || switch) {
                    "must be sorted by chromosome"
                }

                chromosomes.add(section.chrom)

                if (section.isEmpty()) {
                    return
                }

                require(section.start >= edge || switch) { "must be sorted by offset" }
                sum += section.span
                count++

                previous = section.chrom
                edge = section.start
            }
        }

        /**
         * Creates a BigWIG file from given sections.
         *
         * @param wigSections sections sorted by chromosome *and* start offset.
         *                    The method traverses the sections twice:
         *                    firstly to summarize and secondly to write
         *                    and index.
         * @param chromSizes chromosome names and sizes, e.g.
         *                   `("chrX", 59373566)`. Sections on chromosomes
         *                   missing from this list will be dropped.
         * @param zoomLevelCount number of zoom levels to pre-compute.
         *                       Defaults to `8`.
         * @param outputPath BigWIG file path.
         * @param compression method for data sections, see [CompressionType].
         * @param order byte order used, see [java.nio.ByteOrder].
         * @throws IOException if any of the read or write operations failed.
         */
        @Throws(IOException::class)
        @JvmStatic
        @JvmOverloads
        fun write(
                wigSections: Iterable<WigSection>,
                chromSizes: Iterable<Pair<String, Int>>,
                outputPath: Path, zoomLevelCount: Int = 8,
                compression: CompressionType = CompressionType.SNAPPY,
                order: ByteOrder = ByteOrder.nativeOrder(),
                cancelledChecker: (() -> Unit)? = null) {
            val summary = WigSectionSummary().apply { wigSections.forEach { this(it) } }

            val header = OrderedDataOutput(outputPath, order).use { output ->
                output.skipBytes(BigFile.Header.BYTES)
                output.skipBytes(ZoomLevel.BYTES * zoomLevelCount)
                val totalSummaryOffset = output.tell()
                output.skipBytes(BigSummary.BYTES)

                val unsortedChromosomes = chromSizes.filter { it.first in summary.chromosomes }
                        .mapIndexed { i, (key, size) -> BPlusLeaf(key, i, size) }
                val chromTreeOffset = output.tell()
                BPlusTree.write(output, unsortedChromosomes)

                val unzoomedDataOffset = output.tell()
                val resolver = unsortedChromosomes.map { it.key to it.id }.toMap()
                val leaves = ArrayList<RTreeIndexLeaf>(wigSections.map { it.size }.sum())
                var uncompressBufSize = 0

                cancelledChecker?.invoke()

                for ((name, sections) in wigSections.asSequence().groupingBy { it.chrom }) {
                    cancelledChecker?.invoke()

                    val chromIx = resolver[name]
                    if (chromIx == null) {
                        sections.forEach {}  // Consume.
                        continue
                    }

                    for (section in sections.flatMap { it.splice() }) {
                        if (section.isEmpty()) {
                            continue
                        }

                        val dataOffset = output.tell()
                        val current = output.with(compression) {
                            when (section) {
                                is BedGraphSection -> section.write(this, resolver)
                                is FixedStepSection -> section.write(this, resolver)
                                is VariableStepSection -> section.write(this, resolver)
                            }
                        }

                        leaves.add(RTreeIndexLeaf(Interval(chromIx, section.start, section.end),
                                                  dataOffset, output.tell() - dataOffset))
                        uncompressBufSize = Math.max(uncompressBufSize, current)
                    }
                }

                cancelledChecker?.invoke()
                val unzoomedIndexOffset = output.tell()
                RTreeIndex.write(output, leaves, itemsPerSlot = 1)
                BigFile.Header(
                        output.order, MAGIC,
                        version = if (compression == CompressionType.SNAPPY) 5 else 4,
                        zoomLevelCount = zoomLevelCount,
                        chromTreeOffset = chromTreeOffset,
                        unzoomedDataOffset = unzoomedDataOffset,
                        unzoomedIndexOffset = unzoomedIndexOffset,
                        fieldCount = 0, definedFieldCount = 0,
                        totalSummaryOffset = totalSummaryOffset,
                        uncompressBufSize = if (compression.absent) 0 else uncompressBufSize)
            }

            OrderedDataOutput(outputPath, order, create = false).use { header.write(it) }

            with(summary) {
                if (count > 0) {
                    val initial = Math.max(sum divCeiling count.toLong(), 1).toInt() * 10
                    BigFile.Post.zoom(outputPath, initial = initial, cancelledChecker = cancelledChecker)
                }
            }

            BigFile.Post.totalSummary(outputPath)
        }
    }
}

private fun BedGraphSection.write(output: OrderedDataOutput, resolver: Map<String, Int>) {
    with(output) {
        writeInt(resolver[chrom]!!)
        writeInt(start)
        writeInt(end)
        writeInt(0)   // not applicable.
        writeInt(span)
        writeByte(WigSection.Type.BED_GRAPH.ordinal + 1)
        writeByte(0)  // reserved.
        writeShort(size)
        for (i in 0 until size) {
            writeInt(startOffsets[i])
            writeInt(endOffsets[i])
            writeFloat(values[i])
        }
    }
}

private fun FixedStepSection.write(output: OrderedDataOutput, resolver: Map<String, Int>) {
    with(output) {
        writeInt(resolver[chrom]!!)
        writeInt(start)
        writeInt(end)
        writeInt(step)
        writeInt(span)
        writeByte(WigSection.Type.FIXED_STEP.ordinal + 1)
        writeByte(0)  // reserved.
        writeShort(size)
        for (i in 0 until size) {
            writeFloat(values[i])
        }
    }
}

private fun VariableStepSection.write(output: OrderedDataOutput, resolver: Map<String, Int>) {
    with(output) {
        writeInt(resolver[chrom]!!)
        writeInt(start)
        writeInt(end)
        writeInt(0)   // not applicable.
        writeInt(span)
        writeByte(WigSection.Type.VARIABLE_STEP.ordinal + 1)
        writeByte(0)  // reserved.
        writeShort(size)
        for (i in 0 until size) {
            writeInt(positions[i])
            writeFloat(values[i])
        }
    }
}
