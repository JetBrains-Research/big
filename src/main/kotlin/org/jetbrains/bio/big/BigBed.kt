package org.jetbrains.bio.big

import com.google.common.collect.Lists
import com.google.common.primitives.Ints
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.ArrayList
import java.util.Collections
import kotlin.platform.platformStatic

/**
 * Just like BED only BIGGER.
 */
public class BigBedFile throws(IOException::class) protected constructor(path: Path) :
        BigFile<BedEntry>(path, magic = BigBedFile.MAGIC) {

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

                    sb.append(ch)
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
                                        compressed: Boolean = false) {
            SeekableDataOutput.of(outputPath).use { output ->
                output.skipBytes(0, BigFile.Header.BYTES)

                val unsortedChromosomes
                        = Files.readAllLines(chromSizesPath).mapIndexed { i, line ->
                    val chunks = line.split('\t', limit = 2)
                    BPlusLeaf(chunks[0], i, chunks[1].toInt())
                }

                val chromTreeOffset = output.tell()
                BPlusTree.write(output, unsortedChromosomes)

                // XXX move to 'BedFile'?
                val unzoomedDataOffset = output.tell()
                val resolver = unsortedChromosomes.map { it.key to it.id }.toMap()
                val leaves = Lists.newArrayList<RTreeIndexLeaf>()
                var uncompressBufSize = 0
                bedEntries.groupBy { it.name }.forEach { entry ->
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
                                writeBytes(item.rest)
                                writeByte(0)  // null-terminated.

                                end = Math.max(end, item.end)
                            }
                        }

                        leaves.add(RTreeIndexLeaf(Interval.of(chromId, start, end),
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