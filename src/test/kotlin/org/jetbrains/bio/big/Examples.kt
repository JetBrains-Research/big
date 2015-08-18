package org.jetbrains.bio.big

import java.nio.file.Path
import java.nio.file.Paths
import kotlin.platform.platformStatic

object Examples {
    platformStatic fun get(name: String): Path {
        val url = Examples.javaClass.getClassLoader().getResource(name)
                ?: throw IllegalStateException("resource not found")

        return Paths.get(url.toURI()).toFile().toPath()
    }
}