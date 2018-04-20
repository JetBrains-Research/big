package org.jetbrains.bio

import com.indeed.util.mmap.MMapBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path

class MMBRomBufferFactory(
        val path: Path,
        override var order: ByteOrder
) : RomBufferFactory {

    private val memBuffer = MMapBuffer(path, FileChannel.MapMode.READ_ONLY, order)

    override fun create(): RomBuffer = MMBRomBuffer(memBuffer)

    override fun close() {
        memBuffer.close()
    }
}