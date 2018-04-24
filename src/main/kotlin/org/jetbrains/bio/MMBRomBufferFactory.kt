package org.jetbrains.bio

import com.indeed.util.mmap.MMapBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.file.Path

class MMBRomBufferFactory(
        val path: Path,
        order: ByteOrder
) : RomBufferFactory {

    private var memBuffer = MMapBuffer(path, FileChannel.MapMode.READ_ONLY, order)

    override var order = order
        set(value) {
            if (value != field) {
                memBuffer.close()
                memBuffer = MMapBuffer(path, FileChannel.MapMode.READ_ONLY, value)
                field = value
            }
        }

    override fun create(): RomBuffer = MMBRomBuffer(memBuffer)

    override fun close() {
        memBuffer.close()
    }
}