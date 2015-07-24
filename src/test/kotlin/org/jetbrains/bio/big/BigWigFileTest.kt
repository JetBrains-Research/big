package org.jetbrains.bio.big

import org.junit.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue

public class BigWigFileTest {
    Test fun testWriteReadCompressed() = testWriteRead(true)

    Test fun testWriteReadUncompressed() = testWriteRead(false)

    private fun testWriteRead(compressed: Boolean) {
        val path = Files.createTempFile("example", ".bw")
        try {
            val wigSections = WigParser(Examples.get("example.wig").toFile().bufferedReader())
                    .map { it.second }
                    .toList()
            BigWigFile.write(wigSections, Examples.get("hg19.chrom.sizes"),
                                         path, compressed = compressed)

            BigWigFile.read(path).use { bwf ->
                assertEquals(wigSections, bwf.query("chr19", 0, 0).toList())
            }
        } finally {
            Files.deleteIfExists(path)
        }
    }

    Test fun testCompressedExample2() {
        assertVariableStep(Examples.get("example2.bw"),
                           "chr21", 9411191, 50f, 48119895, 60f)
    }

    Test fun testVariableStep() {
        assertVariableStep(Examples.get("variable_step.bw"),
                           "chr2", 300701, 12.5f, 300705, 12.5f)
    }

    Test fun testVariableStepWithSpan() {
        assertVariableStep(Examples.get("variable_step_with_span.bw"),
                           "chr2", 300701, 12.5f, 300705, 12.5f)
    }

    Test fun testFixedStep() {
        assertFixedStep(Examples.get("fixed_step.bw"),
                        "chr3", 400601, 11f, 400801, 33f)
    }

    Test fun testFixedStepWithSpan() {
        assertFixedStep(Examples.get("fixed_step_with_span.bw"),
                        "chr3", 400601, 11f, 400805, 33f)
    }

    private fun assertVariableStep(path: Path, chromosome: String,
                                   position1: Int, value1: Float,
                                   position2: Int, value2: Float) {
        val steps = assertChromosome(path, chromosome)
        assertVariableStep(steps.first(), steps.last(),
                           position1, value1, position2, value2)
    }

    private fun assertFixedStep(path: Path, chromosome: String,
                                position1: Int, value1: Float,
                                position2: Int, value2: Float) {
        val steps = assertChromosome(path, chromosome)
        assertFixedStep(steps.first(), steps.last(),
                        position1, value1, position2, value2)
    }

    private fun assertChromosome(path: Path, chromosome: String): List<WigSection> {
        val file = BigWigFile.read(path)
        val chromosomes = file.chromosomes

        assertEquals(1, chromosomes.size())
        assertEquals(chromosome, chromosomes.values().first())

        val steps = file.query(chromosome, 0, 0).toList()
        assertTrue(steps.isNotEmpty())

        return steps
    }

    private fun assertVariableStep(firstStep: WigSection, lastStep: WigSection,
                                   position1: Int, value1: Float,
                                   position2: Int, value2: Float) {
        assertTrue(firstStep is VariableStepSection)
        assertTrue(lastStep is VariableStepSection)

        assertEquals(position1, firstStep.start + 1)
        assertEquals(value1, firstStep.query().first().score)
        assertEquals(position2, lastStep.end)
        assertEquals(value2, lastStep.query().last().score)
    }

    private fun assertFixedStep(firstStep: WigSection, lastStep: WigSection,
                                position1: Int, value1: Float,
                                position2: Int, value2: Float) {
        assertTrue(firstStep is FixedStepSection)
        assertTrue(lastStep is FixedStepSection)

        assertEquals(position1, firstStep.start + 1)
        assertEquals(value1, firstStep.query().first().score)
        assertEquals(position2, lastStep.end)
        assertEquals(value2, lastStep.query().last().score)
    }
}