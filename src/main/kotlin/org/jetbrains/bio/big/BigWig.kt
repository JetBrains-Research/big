package org.jetbrains.bio.big

import org.jetbrains.bio.CompressionType
import org.jetbrains.bio.OrderedDataOutput
import org.jetbrains.bio.RomBuffer
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.*

/**
 * Bigger brother of the good-old WIG format.
 */
class BigWigFile private constructor(input: RomBuffer,
                                     header: Header,
                                     zoomLevels: List<ZoomLevel>,
                                     bPlusTree: BPlusTree,
                                     rTree: RTreeIndex)
:
        BigFile<WigSection>(input, header, zoomLevels, bPlusTree, rTree) {

    override fun duplicate(): BigWigFile {
        return BigWigFile(input.duplicate(), header, zoomLevels, bPlusTree, rTree)
    }

    override fun summarizeInternal(query: ChromosomeInterval,
                                   numBins: Int): Sequence<IndexedValue<BigSummary>> {
        val wigItems = query(query, overlaps = true).flatMap { it.query() }.toList()
        var edge = 0
        return query.slice(numBins).mapIndexed { i, bin ->
            val summary = BigSummary()
            for (j in edge..wigItems.size - 1) {
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
                                   interval.intersectionLength(bin),
                                   interval.length())
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

    override fun queryInternal(dataOffset: Long, dataSize: Long,
                               query: ChromosomeInterval,
                               overlaps: Boolean): Sequence<WigSection> {
        val chrom = chromosomes[query.chromIx]
        return sequenceOf(input.with(dataOffset, dataSize, compression) {
            val chromIx = getInt()
            assert(chromIx == query.chromIx) { "section contains wrong chromosome" }
            val start = getInt()
            getInt()   // end.
            val step = getInt()
            val span = getInt()
            val type = getUnsignedByte()
            get()  // reserved.
            val count = getUnsignedShort()

            val types = WigSection.Type.values()
            check(type >= 1 && type <= types.size)
            when (types[type - 1]) {
                WigSection.Type.BED_GRAPH -> {
                    val section = BedGraphSection(chrom)
                    for (i in 0..count - 1) {
                        val startOffset = getInt()
                        val endOffset = getInt()
                        val value = getFloat()
                        if (query.contains(startOffset, endOffset, overlaps)) {
                            section[startOffset, endOffset] = value
                        }
                    }

                    section
                }
                WigSection.Type.VARIABLE_STEP -> {
                    val section = VariableStepSection(chrom, span)
                    for (i in 0..count - 1) {
                        val pos = getInt()
                        val value = getFloat()
                        if (query.contains(pos, pos + span, overlaps)) {
                            section[pos] = value
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
                        overlaps   -> -margin        // align to the left.
                        else       -> step - margin  // align to the right.
                    }

                    val realignedStart = Math.max(start, query.startOffset + shift)
                    val section = FixedStepSection(chrom, realignedStart, step, span)
                    for (i in 0..count - 1) {
                        val pos = start + i * step
                        val value = getFloat()
                        if (query.contains(pos, pos + span, overlaps)) {
                            section.add(value)
                        }
                    }

                    section
                }
            }
        })
    }

    companion object {
        /** Magic number used for determining [ByteOrder]. */
        internal val MAGIC: Int = 0x888FFC26.toInt()

        @Throws(IOException::class)
        @JvmStatic fun read(path: Path): BigWigFile {
            val input = RomBuffer(path)
            val header = Header.read(input, MAGIC)
            val zoomLevels = (0..header.zoomLevelCount - 1)
                    .map { ZoomLevel.read(input) }
            val bPlusTree = BPlusTree.read(input, header.chromTreeOffset)
            val rTree = RTreeIndex.read(input, header.unzoomedIndexOffset)
            return BigWigFile(input, header, zoomLevels, bPlusTree, rTree)
        }

        /**
         * Creates a BigWIG file from given sections.
         *
         * @param wigSections sections to write and index.
         * @param chromSizes chromosome names and sizes, e.g.
         *                   `("chrX", 59373566)`.
         * @param zoomLevelCount number of zoom levels to pre-compute.
         *                       Defaults to `8`.
         * @param outputPath BigWIG file path.
         * @param compression method for data sections, see [CompressionType].
         * @param order byte order used, see [java.nio.ByteOrder].
         * @@throws IOException if any of the read or write operations failed.
         */
        @JvmStatic @JvmOverloads fun write(
                wigSections: Iterable<WigSection>,
                chromSizes: Iterable<Pair<String, Int>>,
                outputPath: Path, zoomLevelCount: Int = 8,
                compression: CompressionType = CompressionType.SNAPPY,
                order: ByteOrder = ByteOrder.nativeOrder()) {
            val groupedSections = wigSections.groupBy { it.chrom }
            val header = OrderedDataOutput.of(outputPath, order).use { output ->
                output.skipBytes(BigFile.Header.BYTES)
                output.skipBytes(ZoomLevel.BYTES * zoomLevelCount)
                val totalSummaryOffset = output.tell()
                output.skipBytes(BigSummary.BYTES)

                val unsortedChromosomes = chromSizes.mapIndexed { i, p ->
                    BPlusLeaf(p.first, i, p.second)
                }.filter { it.key in groupedSections }
                val chromTreeOffset = output.tell()
                BPlusTree.write(output, unsortedChromosomes)

                val unzoomedDataOffset = output.tell()
                val resolver = unsortedChromosomes.map { it.key to it.id }.toMap()
                val leaves = ArrayList<RTreeIndexLeaf>(wigSections.map { it.size() }.sum())
                var uncompressBufSize = 0
                for ((name, sections) in groupedSections) {
                    Collections.sort(sections)

                    val chromId = resolver[name]!!
                    for (section in sections.asSequence().flatMap { it.splice() }) {
                        val dataOffset = output.tell()
                        val current = output.with(compression) {
                            when (section) {
                                is BedGraphSection -> section.write(this, resolver)
                                is FixedStepSection -> section.write(this, resolver)
                                is VariableStepSection -> section.write(this, resolver)
                            }
                        }

                        leaves.add(RTreeIndexLeaf(Interval(chromId, section.start, section.end),
                                                  dataOffset, output.tell() - dataOffset))
                        uncompressBufSize = Math.max(uncompressBufSize, current)
                    }
                }

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

            OrderedDataOutput.of(outputPath, order).use { header.write(it) }

            var count = 0
            var sum = 0L
            for (section in groupedSections.values.flatten()) {
                sum += section.span
                count++
            }

            // XXX this can be precomputed with a single pass along with the
            // chromosomes used in the source WIG.
            val initial = Math.max((sum.toDouble() / count).toInt(), 1) * 8
            BigFile.Post.zoom(outputPath, initial = initial)
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
        writeShort(size())
        for (i in 0..size() - 1) {
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
        writeShort(size())
        for (i in 0..size() - 1) {
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
        writeShort(size())
        for (i in 0..size() - 1) {
            writeInt(positions[i])
            writeFloat(values[i])
        }
    }
}
