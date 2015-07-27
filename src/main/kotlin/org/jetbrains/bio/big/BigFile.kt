package org.jetbrains.bio.big

import com.google.common.primitives.Longs
import com.google.common.primitives.Shorts
import gnu.trove.map.TIntObjectMap
import org.apache.commons.math3.stat.descriptive.StatisticalSummary
import java.io.Closeable
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import kotlin.properties.Delegates

/**
 * A common superclass for Big files.
 */
abstract class BigFile<T> protected constructor(path: Path, magic: Int) :
        Closeable, AutoCloseable {

    val input: SeekableDataInput = SeekableDataInput.of(path)
    val header: Header = Header.read(input, magic)
    val zoomLevels: List<ZoomLevel> = (0 until header.zoomLevelCount.toInt()).asSequence()
            .map { ZoomLevel.read(input) }.toList()
    val bPlusTree: BPlusTree
    val rTree: RTreeIndex

    init {
        // Skip AutoSQL string if any.
        while (header.asOffset > 0 && input.readByte() != 0.toByte()) {}

        // Skip total summary block.
        if (header.totalSummaryOffset > 0) {
            Summary.read(input)
        }

        // Skip extended header. Ideally, we should issue a warning
        // if extensions are present.
        if (header.extendedHeaderOffset > 0) {
            with(input) {
                skipBytes(Shorts.BYTES)  // extensionSize.
                val extraIndexCount = readUnsignedShort()
                skipBytes(Longs.BYTES)   // extraIndexListOffset.
                skipBytes(48)            // reserved.

                for (i in 0 until extraIndexCount) {
                    val type = readUnsignedShort()
                    assert(type == 0)
                    val extraFieldCount = readUnsignedShort()
                    skipBytes(Longs.BYTES)     // indexOffset.
                    skipBytes(extraFieldCount *
                              (Shorts.BYTES +  // fieldId,
                               Shorts.BYTES))  // reserved.
                }
            }
        }

        bPlusTree = BPlusTree.read(input, header.chromTreeOffset)
        check(bPlusTree.header.order == header.order)
        rTree = RTreeIndex.read(input, header.unzoomedIndexOffset)
        check(rTree.header.order == header.order)
    }

    public val chromosomes: TIntObjectMap<String> by Delegates.lazy {
        bPlusTree.toMap(input)
    }

    public val compressed: Boolean get() {
        // Compression was introduced in version 3 of the format. See
        // bbiFile.h in UCSC sources.
        return header.version >= 3 && header.uncompressBufSize > 0
    }

    /**
     * Queries an R+-tree.
     *
     * @param name human-readable chromosome name, e.g. `"chr9"`.
     * @param startOffset 0-based start offset (inclusive).
     * @param endOffset 0-based end offset (exclusive), if 0 than the whole
     *                  chromosome is used.
     * @return a list of intervals completely contained within the query.
     * @throws IOException if the underlying [SeekableDataInput] does so.
     */
    throws(IOException::class)
    public fun query(name: String, startOffset: Int, endOffset: Int): Sequence<T> {
        val res = bPlusTree.find(input, name)
        return if (res == null) {
            emptySequence()
        } else {
            val (_key, chromIx, size) = res
            val query = Interval.of(chromIx, startOffset,
                                    if (endOffset == 0) size else endOffset)
            rTree.findOverlappingBlocks(input, query)
                    .flatMap { queryInternal(it.dataOffset, it.dataSize, query) }
        }
    }

    throws(IOException::class)
    protected abstract fun queryInternal(dataOffset: Long, dataSize: Long,
                                         query: ChromosomeInterval): Sequence<T>

    throws(IOException::class)
    override fun close() = input.close()

    class Header(val order: ByteOrder, val version: Int = 4, val zoomLevelCount: Int = 0,
                 val chromTreeOffset: Long, val unzoomedDataOffset: Long,
                 val unzoomedIndexOffset: Long, val fieldCount: Int,
                 val definedFieldCount: Int, val asOffset: Long = 0,
                 val totalSummaryOffset: Long = 0, val uncompressBufSize: Int,
                 val extendedHeaderOffset: Long = 0) {

        fun write(output: SeekableDataOutput, magic: Int) = with(output) {
            seek(0L)  // a header is always first.
            writeInt(magic)
            writeShort(version)
            writeShort(zoomLevelCount)
            writeLong(chromTreeOffset)
            writeLong(unzoomedDataOffset)
            writeLong(unzoomedIndexOffset)
            writeShort(fieldCount.toInt())
            writeShort(definedFieldCount)
            writeLong(asOffset)
            writeLong(totalSummaryOffset)
            writeInt(uncompressBufSize)
            writeLong(extendedHeaderOffset)
        }

        companion object {
            /** Number of bytes used for this header. */
            val BYTES = 64

            fun read(input: SeekableDataInput, magic: Int): Header = with(input) {
                guess(magic)

                val version = readUnsignedShort()
                val zoomLevelCount = readUnsignedShort()
                val chromTreeOffset = readLong()
                val unzoomedDataOffset = readLong()
                val unzoomedIndexOffset = readLong()
                val fieldCount = readUnsignedShort()
                val definedFieldCount = readUnsignedShort()
                val asOffset = readLong()
                val totalSummaryOffset = readLong()
                val uncompressBufSize = readInt()
                val extendedHeaderOffset = readLong()
                return Header(order, version, zoomLevelCount, chromTreeOffset,
                              unzoomedDataOffset, unzoomedIndexOffset,
                              fieldCount, definedFieldCount, asOffset,
                              totalSummaryOffset, uncompressBufSize,
                              extendedHeaderOffset)
            }
        }
    }
}

data class ZoomLevel(public val reductionLevel: Int,
                     public val dataOffset: Long,
                     public val indexOffset: Long) {
    companion object {
        fun read(input: SeekableDataInput) = with(input) {
            val reductionLevel = readInt()
            val reserved = readInt()
            check(reserved == 0)
            val dataOffset = readLong()
            val indexOffset = readLong()
            ZoomLevel(reductionLevel, dataOffset, indexOffset)
        }
    }
}

data class Summary(public val basesCovered: Long,
                   public val minVal: Double,
                   public val maxVal: Double,
                   public val sumData: Double,
                   public val sumSquared: Double) {
    companion object {
        fun read(input: SeekableDataInput) = with(input) {
            val basesCovered = readLong()
            val minVal = readDouble()
            val maxVal = readDouble()
            val sumData = readDouble()
            val sumSquared = readDouble()
            Summary(basesCovered, minVal, maxVal, sumData, sumSquared)
        }
    }
}