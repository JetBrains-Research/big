package org.jbb.big

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.platform.platformStatic

/**
 * Just like BED only BIGGER.
 */
public class BigBedFile throws(IOException::class) protected constructor(path: Path) :
        BigFile<BedData>(path) {

    override fun getHeaderMagic(): Int = MAGIC

    throws(IOException::class)
    override fun queryInternal(query: ChromosomeInterval): Sequence<BedData> {
        val chrom = chromosomes[query.chromIx]
        return header.rTree.findOverlappingBlocks(handle, query).flatMap { block ->
            val (_interval, dataOffset, dataSize) = block
            handle.with(dataOffset, dataSize, isCompressed()) {
                val chunk = ArrayList<BedData>()
                do {
                    assert(readInt() == query.chromIx, "interval contains wrong chromosome")
                    val startOffset = readInt()
                    val endOffset = readInt()
                    val sb = StringBuilder()
                    while (true) {
                        var ch = readByte().toInt()
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

                    chunk.add(BedData(chrom, startOffset, endOffset, sb.toString()))
                } while (tell() - dataOffset < dataSize)

                chunk.asSequence()
            }
        }
    }

    companion object {
        public val MAGIC: Int = 0x8789F2EB.toInt()

        throws(IOException::class)
        public platformStatic fun read(path: Path): BigBedFile = BigBedFile(path)
    }
}

class BedFile(private val path: Path) : Iterable<BedData> {
    override fun iterator(): Iterator<BedData> = Files.lines(path).map { line ->
        val chunks = line.split('\t', limit = 4)
        BedData(chunks[0], chunks[1].toInt(), chunks[2].toInt(),
                if (chunks.size() == 3) "" else chunks[3])
    }.iterator()

    companion object {
        throws(IOException::class)
        public platformStatic fun read(path: Path): BedFile = BedFile(path)
    }
}

/**
 * A minimal representation of a BED file entry.
 */
public data class BedData(
        /** Chromosome name, e.g. `"chr9"`. */
        public val name: String,
        /** 0-based start offset (inclusive). */
        public val start: Int,
        /** 0-based end offset (exclusive). */
        public val end: Int,
        /** Comma-separated string of additional BED values. */
        public val rest: String = "")