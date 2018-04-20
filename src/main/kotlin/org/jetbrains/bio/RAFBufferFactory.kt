package org.jetbrains.bio

import java.nio.ByteOrder
import java.nio.file.Path

@Deprecated("Use seekable stream")
class RAFBufferFactory(
        private val path: Path,
        override var order: ByteOrder,
        private val bufferSize: Int = -1
) : RomBufferFactory {
    override fun create(): RomBuffer = RAFBuffer(path, order, bufferSize = bufferSize)

    override fun close() {
        // Do nothing
    }
}