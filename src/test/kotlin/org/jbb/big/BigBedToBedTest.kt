package org.jbb.big

import org.junit.Test
import java.io.Closeable
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

public class BigBedToBedTest {
    Test fun testBigBedToBed() {
        val inputPath = Examples.get("example1.bb")
        val outputPath = Files.createTempFile("out", ".bed")
        BigBedToBed.main(inputPath, outputPath, "", 0, 0, 0)
        try {
            assertTrue(Files.exists(outputPath))
            assertTrue(Files.size(outputPath) > 0)
        } finally {
            Files.deleteIfExists(outputPath)
        }
    }

    Test fun testBigBedToBedFilterByChromosomeName() {
        val inputPath = Examples.get("example1.bb")
        val outputPath = Files.createTempFile("out", ".bed")
        val chromStart = 0
        val chromEnd = 0
        val maxItems = 0

        // In example1.bb we have only chr21 chromosome
        BigBedToBed.main(inputPath, outputPath, "chr22", chromStart, chromEnd, maxItems)
        try {
            assertTrue(Files.exists(outputPath))
            assertFalse(Files.size(outputPath) > 0)
        } finally {
            Files.deleteIfExists(outputPath)
        }

        // This chromosome exist in example bb-file
        BigBedToBed.main(inputPath, outputPath, "chr21", chromStart, chromEnd, maxItems)
        try {
            assertTrue(Files.exists(outputPath))
            assertTrue(Files.size(outputPath) > 0)
        } finally {
            Files.deleteIfExists(outputPath)
        }

        // Get all chromosome from example file
        BigBedToBed.main(inputPath, outputPath, "", chromStart, chromEnd, maxItems)
        try {
            assertTrue(Files.exists(outputPath))
            assertTrue(Files.size(outputPath) > 0)
        } finally {
            Files.deleteIfExists(outputPath)
        }
    }

    Test fun testBigBedToBedRestrictOutput() {
        val inputPath = Examples.get("example1.bb")
        val outputPath = Files.createTempFile("out", ".bed")

        // In example1.bb we have only one chromosome
        val chromName = "chr21"
        var maxItems = 10

        // Check lines count in output file
        BigBedToBed.main(inputPath, outputPath, chromName, 0, 0, maxItems)
        try {
            lines(outputPath).use { lines ->
                assertTrue(Files.exists(outputPath))
                assertTrue(Files.size(outputPath) > 0)
                assertEquals(maxItems.toLong(), lines.wrapped.count())
            }
        } finally {
            Files.deleteIfExists(outputPath)
        }

        // Restrict intervals
        BigBedToBed.main(inputPath, outputPath, chromName, 9508110, 9906613, maxItems)
        try {
            lines(outputPath).use { lines ->
                assertTrue(Files.exists(outputPath))
                assertTrue(Files.size(outputPath) > 0)
                assertEquals(5L, lines.wrapped.count())
            }
        } finally {
            Files.deleteIfExists(outputPath)
        }

        BigBedToBed.main(inputPath, outputPath, chromName, 9508110, 9906612, maxItems)
        try {
            lines(outputPath).use { lines ->
                assertTrue(Files.exists(outputPath))
                assertTrue(Files.size(outputPath) > 0)
                assertEquals(4L, lines.wrapped.count())
            }
        } finally {
            Files.deleteIfExists(outputPath)
        }

        BigBedToBed.main(inputPath, outputPath, chromName, 9508110, 9906614, maxItems)
        try {
            lines(outputPath).use { lines ->
                assertTrue(Files.exists(outputPath))
                assertTrue(Files.size(outputPath) > 0)
                assertEquals(5L, lines.wrapped.count())
            }
        } finally {
            Files.deleteIfExists(outputPath)
        }

        BigBedToBed.main(inputPath, outputPath, chromName, 9508110, 9903230, 3)
        try {
            lines(outputPath).use { lines ->
                assertTrue(Files.exists(outputPath))
                assertTrue(Files.size(outputPath) > 0)
                assertEquals(3L, lines.wrapped.count())
            }
        } finally {
            Files.deleteIfExists(outputPath)
        }
    }
}

// XXX that is to overcome the lack of AutoCloseable in Kotlin.
private class Wrapper<T : AutoCloseable>(public val wrapped: T) : Closeable {
    override fun close() = wrapped.close()
}

private fun lines(path: Path): Wrapper<java.util.stream.Stream<String>> {
    return Wrapper(Files.lines(path))
}