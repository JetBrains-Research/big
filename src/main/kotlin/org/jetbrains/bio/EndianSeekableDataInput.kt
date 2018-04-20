package org.jetbrains.bio

import java.nio.ByteOrder

/**
 * @author Roman.Chernyatchik
 */
interface EndianSeekableDataInput : SeekableDataInput {
    var order: ByteOrder
}