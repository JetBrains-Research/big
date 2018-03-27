package org.jetbrains.bio.big

import org.jetbrains.bio.RomBuffer

/**
 * @author Roman.Chernyatchik
 */
interface RomBufferFactory {
    fun create(): RomBuffer
    fun close()
}