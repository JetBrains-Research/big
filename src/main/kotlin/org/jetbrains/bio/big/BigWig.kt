package org.jetbrains.bio.big

import com.google.common.collect.Lists
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import kotlin.platform.platformStatic

/**
 * Bigger brother of the good-old WIG format.
 */
public class BigWigFile @throws(IOException::class) protected constructor(path: Path) :
        BigFile<WigSection>(path, magic = BigWigFile.MAGIC) {

    override fun summarizeInternal(query: ChromosomeInterval,
                                   numBins: Int): Sequence<Pair<Int, BigSummary>> {
        val wigItems = query(query).flatMap { it.query().asSequence() }.toList()
        var edge = 0
        return query.slice(numBins).mapIndexed { i, bin ->
            val summary = BigSummary()
            for (j in edge until wigItems.size()) {
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
                                   (interval intersection bin).length(),
                                   interval.length())
                }
            }

            if (summary.isEmpty()) null else i to summary
        }.filterNotNull()
    }

    private fun ChromosomeInterval.contains(pos: Int, span: Int): Boolean {
        return Interval(chromIx, pos, pos + span) in this
    }

    override fun queryInternal(dataOffset: Long, dataSize: Long,
                               query: ChromosomeInterval): Sequence<WigSection> {
        val chrom = chromosomes[query.chromIx]
        return sequenceOf(input.with(dataOffset, dataSize, compressed) {
            val chromIx = readInt()
            assert(chromIx == query.chromIx, "section contains wrong chromosome")
            val start = readInt()
            readInt()   // end.
            val step = readInt()
            val span = readInt()
            val type = readUnsignedByte()
            readByte()  // reserved.
            val count = readUnsignedShort()

            val types = WigSection.Type.values()
            check(type >= 1 && type <= types.size())
            when (types[type - 1]) {
                WigSection.Type.BED_GRAPH ->
                    throw IllegalStateException("bedGraph sections aren't supported")
                WigSection.Type.VARIABLE_STEP -> {
                    val section = VariableStepSection(chrom, span)
                    for (i in 0 until count) {
                        val pos = readInt()
                        val value = readFloat()
                        if (query.contains(pos, span)) {
                            section[pos] = value
                        }
                    }

                    section
                }
                WigSection.Type.FIXED_STEP -> {
                    // Realign query start to the nearest (rightmost) interval.
                    // This ensures that all WIG intervals have proper offsets
                    // and are contained in query.
                    val shift = query.startOffset % step
                    val realignedStart = Math.max(
                            start,
                            query.startOffset + if (shift == 0) 0 else (step - shift))
                    val section = FixedStepSection(chrom, realignedStart, step, span)
                    for (i in 0 until count) {
                        val value = readFloat()
                        if (query.contains(start + i * step, span)) {
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
        val MAGIC: Int = 0x888FFC26.toInt()

        @throws(IOException::class)
        public platformStatic fun read(path: Path): BigWigFile = BigWigFile(path)

        /**
         * Creates a BigWIG file from given sections.
         *
         * @param wigSections sections to write and index.
         * @param chromSizesPath path to the TSV file with chromosome
         *                       names and sizes.
         * @param zoomLevelCount number of zoom levels to pre-compute.
         *                       Defaults to `8`.
         * @param outputPath BigWIG file path.
         * @param compressed compress BigWIG data sections with gzip.
         *                   Defaults to `false`.
         * @param order byte order used, see [java.nio.ByteOrder].
         * @@throws IOException if any of the read or write operations failed.
         */
        @throws(IOException::class)
        public platformStatic fun write(wigSections: Iterable<WigSection>,
                                        chromSizesPath: Path,
                                        outputPath: Path,
                                        zoomLevelCount: Int = 8,
                                        compressed: Boolean = true,
                                        order: ByteOrder = ByteOrder.nativeOrder()) {
            val groupedSections = wigSections.sort().groupBy { it.chrom }
            val header = CountingDataOutput.of(outputPath, order).use { output ->
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
                for ((name, sections) in groupedSections) {
                    val chromId = resolver[name]!!
                    for (section in sections.asSequence().flatMap { it.splice() }) {
                        val dataOffset = output.tell()
                        val current = output.with(compressed) {
                            when (section) {
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
                        output.order, MAGIC, zoomLevelCount = zoomLevelCount,
                        chromTreeOffset = chromTreeOffset,
                        unzoomedDataOffset = unzoomedDataOffset,
                        unzoomedIndexOffset = unzoomedIndexOffset,
                        fieldCount = 0, definedFieldCount = 0,
                        totalSummaryOffset = totalSummaryOffset,
                        uncompressBufSize = if (compressed) uncompressBufSize else 0)
            }

            CountingDataOutput.of(outputPath, order).use { header.write(it) }
            BigFile.Post.zoom(outputPath)
            BigFile.Post.totalSummary(outputPath)
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
        writeByte(WigSection.Type.FIXED_STEP.ordinal() + 1)
        writeByte(0) // reserved.
        writeUnsignedShort(values.size())
        for (i in 0 until values.size()) {
            writeFloat(values[i])
        }
    }
}

private fun VariableStepSection.write(output: OrderedDataOutput, resolver: Map<String, Int>) {
    with(output) {
        writeInt(resolver[chrom]!!)
        writeInt(start)
        writeInt(end)
        writeInt(0)
        writeInt(span)
        writeByte(WigSection.Type.VARIABLE_STEP.ordinal() + 1)
        writeByte(0)  // reserved.
        writeUnsignedShort(values.size())
        for (i in 0 until values.size()) {
            writeInt(positions[i])
            writeFloat(values[i])
        }
    }
}