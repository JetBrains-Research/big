package org.jbb.big

import java.io.*
import java.nio.file.Path
import kotlin.platform.platformStatic

/**
 * Bigger brother of the good-old WIG format.
 */
public class BigWigFile throws(IOException::class) protected constructor(path: Path) :
        BigFile<WigSection>(path, magic = 0x888FFC26.toInt()) {

    throws(IOException::class)
    override fun queryInternal(dataOffset: Long, dataSize: Long,
                               query: ChromosomeInterval): Sequence<WigSection> {
        val chrom = chromosomes[query.chromIx]
        return listOf(input.with(dataOffset, dataSize, compressed) {
            assert(readInt() == query.chromIx, "section contains wrong chromosome")
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
                        val position = readInt()
                        section[position] = readFloat()
                    }

                    section
                }
                WigSection.Type.FIXED_STEP -> {
                    val section = FixedStepSection(chrom, start, step, span)
                    for (i in 0 until count) {
                        section.add(readFloat())
                    }

                    section
                }
            }
        }).asSequence()
    }

    companion object {
        throws(IOException::class)
        public platformStatic fun read(path: Path): BigWigFile = BigWigFile(path)
    }
}