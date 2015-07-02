package org.jbb.big

import gnu.trove.list.TFloatList
import gnu.trove.list.TIntList
import gnu.trove.list.array.TFloatArrayList
import gnu.trove.list.array.TIntArrayList
import java.io.IOException
import java.nio.file.Path
import java.util.*
import kotlin.platform.platformStatic

/**
 * Bigger brother of the good-old WIG format.
 */
public class BigWigFile throws(IOException::class) protected constructor(path: Path) :
        BigFile<WigSection>(path, magic = 0x888FFC26.toInt()) {

    throws(IOException::class)
    override fun queryInternal(dataOffset: Long, dataSize: Long,
                               query: ChromosomeInterval): Sequence<WigSection> {
        return listOf(input.with(dataOffset, dataSize, compressed) {
            WigSection.read(this)
        }).asSequence()
    }

    companion object {
        throws(IOException::class)
        public platformStatic fun read(path: Path): BigWigFile = BigWigFile(path)
    }
}

public interface WigSection {
    val start: Int
    val end: Int
    val values: TFloatList

    public enum class Type(public val id: Int) {
        BED_GRAPH(1),
        VARIABLE_STEP(2),
        FIXED_STEP(3)
    }

    companion object {
        public fun read(input: OrderedDataInput): WigSection = with(input) {
            val chromIx = readInt()
            val start = readInt()
            val end = readInt()
            val step = readInt()
            val span = readInt()
            val type = readUnsignedByte()
            readByte()  // reserved.
            val count = readUnsignedShort()

            val types = Type.values()
            check(type >= 1 && type <= types.size())
            return when (types[type - 1]) {
                Type.BED_GRAPH ->
                    throw IllegalStateException("bedGraph sections aren't supported")
                Type.VARIABLE_STEP -> {
                    val section = VariableStepSection(span)
                    for (i in 0 until count) {
                        val position = readInt()
                        section[position] = readFloat()
                    }

                    section
                }
                Type.FIXED_STEP -> {
                    val section = FixedStepSection(start, step, span)
                    for (i in 0 until count) {
                        section.add(readFloat())
                    }

                    section
                }
            }
        }
    }
}

/**
 * A section with gaps; _variable_ step mean i+1-th range is on
 * arbitrary distance from i-th range. Note, however, that range
 * width remains _fixed_ throughout the track.
 */
public class VariableStepSection(
        /** Range width. */
        public val span: Int = 1) : WigSection {

    override val start: Int get() {
        return if (positions.isEmpty()) 0 else positions[0]
    }

    override val end: Int get() = if (positions.isEmpty()) {
        Integer.MAX_VALUE
    } else {
        positions[positions.size() - 1] + span
    }

    public val positions: TIntList = TIntArrayList()
    public override val values: TFloatList = TFloatArrayList()

    public fun set(position: Int, value: Float) {
        // XXX make sure we're doing insertions in sorted order.
        positions.add(position)
        values.add(value)
    }
}

/**
 * A section with contiguous ranges. Both the distance between consecutive
 * ranges and range width is fixed throughout the track.
 */
public class FixedStepSection(
        /** Start offset of the first range on the track. */
        public override val start: Int,
        /** Distance between consecutive ranges. */
        public val step: Int = 1,
        /** Range width. */
        public val span: Int = 1) : WigSection {

    override val end: Int get() = start + step * (values.size() - 1) + span

    public override val values: TFloatList = TFloatArrayList()

    public fun add(value: Float) {
        values.add(value)
    }
}