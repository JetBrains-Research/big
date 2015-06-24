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
public class BigWigFile @throws(IOException::class) protected constructor(path: Path) :
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

