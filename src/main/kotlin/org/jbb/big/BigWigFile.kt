package org.jbb.big

import com.google.common.collect.Lists
import java.io.IOException
import java.nio.file.Path
import kotlin.platform.platformStatic

/**
 * Bigger brother of good old WIG format.
 *
 * @author Konstantin Kolosovsky
 * @since 12/05/15
 */
public class BigWigFile throws(IOException::class) protected constructor(path: Path) :
        BigFile<WigData>(path) {

    override fun getHeaderMagic(): Int = MAGIC

    throws(IOException::class)
    override fun queryInternal(query: RTreeInterval, maxItems: Int): List<WigData> {
        val res = Lists.newArrayList<WigData>()
        header.rTree.findOverlappingBlocks(handle, query) { block ->
            handle.seek(block.dataOffset);

            if (isCompressed()) {
                handle.startCompressedBlock(block.dataSize);
            }
            try {
                // TODO: Do we need to merge WigData instances with subsequent headers?
                // TODO: Investigate bigWigToWig output and source code.
                res.add(readWigData());
            } finally {
                if (isCompressed()) {
                    handle.endCompressedBlock();
                }
            }

            check(handle.tell() - block.dataOffset == block.dataSize,
                  "WIG section read incorrectly")
        }

        return res
    }

    throws(IOException::class)
    private fun readWigData(): WigData {
        val header = WigSectionHeader.read(handle)
        return when (header.type) {
            WigSectionHeader.FIXED_STEP_TYPE ->
                FixedStepWigData.read(header, handle)
            WigSectionHeader.VARIABLE_STEP_TYPE ->
                VariableStepWigData.read(header, handle)
            WigSectionHeader.BED_GRAPH_TYPE ->
                throw IllegalStateException("bedGraph sections are not supported in bigWig files")
            else ->
                throw IllegalStateException("unknown section type " + header.type)
        }
    }

    companion object {
        public val MAGIC: Int = 0x888FFC26.toInt()

        throws(IOException::class)
        public platformStatic fun parse(path: Path): BigWigFile = BigWigFile(path)
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
                              public val reserved: Byte,
                              public val count: Short) {
    companion object {
        public val BED_GRAPH_TYPE: Byte = 1
        public val VARIABLE_STEP_TYPE: Byte = 2
        public val FIXED_STEP_TYPE: Byte = 3

        throws(IOException::class)
        public fun read(input: SeekableDataInput): WigSectionHeader {
            val id = input.readInt()
            val start = input.readInt()
            val end = input.readInt()
            val step = input.readInt()
            val span = input.readInt()
            val type = input.readByte()
            val reserved = input.readByte()
            val count = input.readShort()
            return WigSectionHeader(id, start, end, step, span, type, reserved, count)
        }
    }
}