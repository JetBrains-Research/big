package org.jbb.big

import com.google.common.collect.Lists
import java.io.IOException
import java.nio.file.Path
import kotlin.platform.platformStatic

/**
 * Just like BED only BIGGER.
 *
 * @author Sergei Lebedev
 * @since 11/04/15
 */
public class BigBedFile throws(IOException::class) protected constructor(path: Path) :
        BigFile<BedData>(path) {

    override fun getHeaderMagic(): Int = MAGIC

    throws(IOException::class)
    override fun queryInternal(query: RTreeInterval, maxItems: Int): List<BedData> {
        val chromIx = query.left.chromIx
        val res = Lists.newArrayList<BedData>()
        header.rTree.findOverlappingBlocks(handle, query) { block ->
            handle.seek(block.dataOffset)

            do {
                assert(handle.readInt() == chromIx, "interval contains wrong chromosome")
                if (maxItems > 0 && res.size() == maxItems) {
                    // XXX think of a way of terminating the traversal?
                    return@findOverlappingBlocks
                }

                val startOffset = handle.readInt()
                val endOffset = handle.readInt()
                val sb = StringBuilder()
                while (true) {
                    var ch = handle.readByte().toInt()
                    if (ch == 0) {
                        break
                    }

                    sb.append(ch)
                }

                // This was somewhat tricky to get right, please make sure
                // you understand the code before modifying it.
                if (startOffset < query.left.offset || endOffset > query.right.offset) {
                    continue
                } else if (startOffset > query.right.offset) {
                    break
                }

                res.add(BedData(chromIx, startOffset, endOffset, sb.toString()))
            } while (handle.tell() - block.dataOffset < block.dataSize)
        }

        return res
    }

    companion object {
        public val MAGIC: Int = 0x8789F2EB.toInt()

        throws(IOException::class)
        public platformStatic fun parse(path: Path): BigBedFile = BigBedFile(path)
    }
}
