package org.jetbrains.bio

import org.jetbrains.bio.big.*
import sun.awt.OSInfo
import java.io.IOException
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

internal object Examples {
    @JvmStatic operator fun get(name: String): Path {
        val url = Examples.javaClass.classLoader.getResource(name)
                  ?: throw IllegalStateException("resource not found")

        return Paths.get(url.toURI()).toFile().toPath()
    }
}

/** Fetches chromosome sizes from a UCSC provided TSV file. */
internal fun Path.chromosomes(): List<Pair<String, Int>> {
    return bufferedReader().lineSequence().map { line ->
        val chunks = line.split('\t', limit = 3)
        chunks[0] to chunks[1].toInt()
    }.toList()
}

internal inline fun withTempFile(prefix: String, suffix: String,
                                 block: (Path) -> Unit) {
    val path = Files.createTempFile(prefix, suffix)
    try {
        block(path)
    } finally {
        try {
            Files.delete(path)
        } catch (e: IOException) {
            // Mmaped buffer not yet garbage collected. Leave it to the VM.
            path.toFile().deleteOnExit()
        }
    }
}

abstract class RomBufferFactoryProvider(private val title: String) {
    abstract operator fun invoke(path: Path, byteOrder: ByteOrder): RomBufferFactory
    override fun toString() = title
}

fun romFactoryProviders(): List<RomBufferFactoryProvider> {
    val providers: MutableList<RomBufferFactoryProvider> =  mutableListOf(
            object : RomBufferFactoryProvider("RAFBufferFactory") {
                override fun invoke(path: Path, byteOrder: ByteOrder) =
                        RAFBufferFactory(path, byteOrder)
            }
    )
    if (OSInfo.getOSType() != OSInfo.OSType.WINDOWS) {
        providers.add(object : RomBufferFactoryProvider("MMBRomBufferFactory") {
            override fun invoke(path: Path, byteOrder: ByteOrder) =
                    MMBRomBufferFactory(path, byteOrder)
        })
    }
    return providers
}

fun BigBedFile.Companion.read(path: Path, provider: RomBufferFactoryProvider) =
        BigBedFile.read(path) { path, byteOrder ->
            provider(path, byteOrder)
        }

fun BigWigFile.Companion.read(path: Path, provider: RomBufferFactoryProvider) =
        BigWigFile.read(path) { path, byteOrder ->
            provider(path, byteOrder)
        }

fun BigFile.Companion.read(path: Path, provider: RomBufferFactoryProvider) =
        BigFile.read(path) { path, byteOrder ->
            provider(path, byteOrder)
        }

fun allBigFileParamsSets(): Iterable<Array<Any>> {
    val params = mutableListOf<Array<Any>>()
    for (factoryProvider in romFactoryProviders()) {
        for (byteOrder in listOf(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)) {
            CompressionType.values().mapTo(params) { arrayOf(byteOrder, it, factoryProvider) }
        }
    }
    return params
}
