package org.jbb.big

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import java.io.Closeable
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Path
import java.util.function.Consumer

/**
 * A common superclass for Big files.
 *
 * TODO: cache chromosome mapping instead of querying the B+ tree
 * every time.
 *
 * @author Sergei Lebedev
 * @author Sergey Zherevchuk
 * @since 11/04/15
 */
abstract class BigFile<T> throws(IOException::class) protected constructor(path: Path) :
        Closeable, AutoCloseable {

    class Header protected constructor(public val byteOrder: ByteOrder,
                                       public val version: Short,
                                       public val unzoomedDataOffset: Long,
                                       public val fieldCount: Short,
                                       public val definedFieldCount: Short,
                                       public val asOffset: Long,
                                       public val totalSummaryOffset: Long,
                                       public val uncompressBufSize: Int,
                                       public val zoomLevels: List<ZoomLevel>,
                                       public val bPlusTree: BPlusTree,
                                       public val rTree: RTreeIndex) {
        companion object {
            throws(IOException::class)
            fun read(input: SeekableDataInput, magic: Int): Header = with(input) {
                guess(magic)

                val version = readShort()
                val zoomLevelCount = readShort().toInt()
                val chromTreeOffset = readLong()
                val unzoomedDataOffset = readLong()
                val unzoomedIndexOffset = readLong()
                val fieldCount = readShort()
                val definedFieldCount = readShort()
                val asOffset = readLong()
                val totalSummaryOffset = readLong()
                val uncompressBufSize = readInt()
                val reserved = readLong()  // currently 0.

                if (uncompressBufSize > 0) {
                    // TODO: Try to use BigFile.supportsCompression() or implement for BigBed
                    // TODO: Check version is >= 3 - see corresponding table in "Supplementary Data".
                    //        throw new IllegalStateException("data compression is not supported");
                }

                check(reserved == 0L, "header extensions are not supported")

                val zoomLevels = (0 until zoomLevelCount).asSequence()
                        .map { ZoomLevel.read(input) }.toList()

                val bpt = BPlusTree.read(input, chromTreeOffset)
                val rti = RTreeIndex.read(input, unzoomedIndexOffset)
                return Header(order(), version, unzoomedDataOffset,
                              fieldCount, definedFieldCount, asOffset,
                              totalSummaryOffset, uncompressBufSize,
                              zoomLevels, bpt, rti)
            }
        }
    }

    // XXX maybe we should make it a DataIO instead of separate
    // Input/Output classes?
    val handle: SeekableDataInput = SeekableDataInput.of(path)
    val header: Header = Header.read(handle, getHeaderMagic())

    throws(IOException::class)
    public fun chromosomes(): Set<String> {
        val b = ImmutableSet.builder<String>()
        header.bPlusTree.traverse(handle, Consumer { b.add(it.key) })
        return b.build()
    }

    /**
     * Queries an R+-tree.
     *
     * @param chromName human-readable chromosome name, e.g. `"chr9"`.
     * @param startOffset 0-based start offset (inclusive).
     * @param endOffset 0-based end offset (exclusive), if 0 than the whole
     *                  chromosome is used.
     * @param maxItems upper bound on the number of items to return.
     * @return a list of intervals completely contained within the query.
     * @throws IOException if the underlying [SeekableDataInput] does so.
     */
    throws(IOException::class)
    public jvmOverloads fun query(chromName: String, startOffset: Int, endOffset: Int,
                                  maxItems: Int = 0): List<T> {
        val res = header.bPlusTree.find(handle, chromName)
        return if (res.isPresent()) {
            val (_key, chromIx, size) = res.get()
            val query = RTreeInterval.of(chromIx, startOffset,
                                         if (endOffset == 0) size else endOffset)
            queryInternal(query, maxItems)
        } else {
            listOf()
        }
    }

    public abstract fun getHeaderMagic(): Int

    public fun isCompressed(): Boolean = header.uncompressBufSize > 0

    throws(IOException::class)
    protected abstract fun queryInternal(query: RTreeInterval, maxItems: Int): List<T>

    throws(IOException::class)
    override fun close() = handle.close()
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