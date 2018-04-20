package org.jetbrains.bio

import java.io.Closeable
import java.nio.ByteOrder

interface RomBufferFactory : Closeable {
    var order: ByteOrder
    fun create(): RomBuffer
    override fun close()
}