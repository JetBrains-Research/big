package org.jbb.big

import com.google.common.collect.Lists
import java.io.IOException
import java.nio.file.Path
import kotlin.platform.platformStatic

/**
 * Bigger brother of good old WIG format.
 */
public class BigWigFile throws(IOException::class) protected constructor(path: Path) :
        BigFile<WigData>(path) {

    override fun getHeaderMagic(): Int = MAGIC

    throws(IOException::class)
    override fun queryInternal(query: ChromosomeInterval, maxItems: Int): List<WigData> {
        val res = Lists.newArrayList<WigData>()
        header.rTree.findOverlappingBlocks(handle, query) { block ->
            handle.seek(block.dataOffset);

            // TODO: Do we need to merge WigData instances with subsequent headers?
            // TODO: Investigate bigWigToWig output and source code.
            val data = if (isCompressed()) {
                handle.compressed(block.dataSize) { readWigData(it) }
            } else {
                readWigData(handle)
            }

            res.add(data);

            check(handle.tell() - block.dataOffset == block.dataSize,
                  "WIG section read incorrectly")
        }

        return res
    }

    throws(IOException::class)
    private fun readWigData(input: SeekableDataInput): WigData {
        val header = WigSectionHeader.read(input)
        return when (header.type) {
            WigSectionHeader.FIXED_STEP_TYPE ->
                FixedStepWigData.read(header, input)
            WigSectionHeader.VARIABLE_STEP_TYPE ->
                VariableStepWigData.read(header, input)
            WigSectionHeader.BED_GRAPH_TYPE ->
                throw IllegalStateException("bedGraph sections are not supported in bigWig files")
            else ->
                throw IllegalStateException("unknown section type " + header.type)
        }
    }

    companion object {
        public val MAGIC: Int = 0x888FFC26.toInt()

        throws(IOException::class)
        public platformStatic fun read(path: Path): BigWigFile = BigWigFile(path)
    }
}

public abstract class WigData protected constructor(val header: WigSectionHeader) {
    abstract val values: FloatArray
}

public class VariableStepWigData protected constructor(header: WigSectionHeader) :
        WigData(header) {

    /**
     * Note. Currently all positions are excluded - +1 should be added
     * to get correct position (for instance to convert from bigWig to
     * WIG format). It is raw representation of bigWig data. Possibly
     * such representation will give some other bonuses. If not this
     * representation could be revised.
     */
    public val positions: IntArray = IntArray(header.count.toInt())
    public override val values: FloatArray = FloatArray(header.count.toInt())

    public fun set(index: Int, position: Int, value: Float) {
        positions[index] = position
        values[index] = value
    }

    companion object {
        throws(IOException::class)
        public fun read(header: WigSectionHeader, input: SeekableDataInput): VariableStepWigData {
            val data = VariableStepWigData(header)
            for (i in 0..header.count - 1) {
                val position = input.readInt()
                data[i, position] = input.readFloat()
            }

            return data
        }
    }
}

public class FixedStepWigData protected constructor(header: WigSectionHeader) :
        WigData(header) {

    public override val values: FloatArray = FloatArray(header.count.toInt())

    public fun set(index: Int, value: Float) {
        values[index] = value
    }

    companion object {
        throws(IOException::class)
        public fun read(header: WigSectionHeader, input: SeekableDataInput): FixedStepWigData {
            val data = FixedStepWigData(header)
            for (i in 0..header.count - 1) {
                val value = input.readFloat()
                data[i] = value
            }

            return data
        }
    }
}

public class WigSectionHeader(public val id: Int,
                              /** Start position (exclusive). */
                              public val start: Int,
                              /** End position (inclusive). */
                              public val end: Int,
                              public val step: Int,
                              public val span: Int,
                              public val type: Byte,
                              public val count: Short) {
    companion object {
        public val BED_GRAPH_TYPE: Byte = 1
        public val VARIABLE_STEP_TYPE: Byte = 2
        public val FIXED_STEP_TYPE: Byte = 3

        throws(IOException::class)
        public fun read(input: SeekableDataInput): WigSectionHeader = with (input) {
            val id = readInt()
            val start = readInt()
            val end = readInt()
            val step = readInt()
            val span = readInt()
            val type = readByte()
            readByte()  // reserved.
            val count = readShort()
            return WigSectionHeader(id, start, end, step, span, type, count)
        }
    }
}