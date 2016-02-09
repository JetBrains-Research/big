package org.jetbrains.bio

enum class CompressionType {
    NO_COMPRESSION,
    DEFLATE,
    SNAPPY;

    val absent: Boolean get() = this == NO_COMPRESSION
}
