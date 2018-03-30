package org.jetbrains.bio

/**
 * @author Roman.Chernyatchik
 */
interface RomBufferFactory {
    fun create(): RomBuffer
    fun close()
}