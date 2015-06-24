package org.jbb.big

import java.nio.file.Path
import java.nio.file.Paths
import kotlin.platform.platformStatic

public object Examples {
    platformStatic fun get(name: String): Path {
        val url = javaClass<Examples>().getClassLoader().getResource(name)
                ?: throw IllegalStateException("resource not found")

        return Paths.get(url.toURI()).toFile().toPath()
    }
}