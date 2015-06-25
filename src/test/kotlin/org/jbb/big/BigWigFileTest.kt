package org.jbb.big

import org.junit.Test
import java.io.IOException
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertTrue

public class BigWigTest {
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

    private fun assertChromosome(path: Path, chromosome: String): List<WigData> {
        val file = BigWigFile.read(path)
        val chromosomes = file.chromosomes()

        assertEquals(1, chromosomes.size())
        assertEquals(chromosome, chromosomes.first())

        val steps = file.query(chromosome, 0, 0)
        assertTrue(steps.isNotEmpty())

        return steps
    }

    private fun assertVariableStep(firstStep: WigData, lastStep: WigData,
                                   position1: Int, value1: Float,
                                   position2: Int, value2: Float) {
        assertTrue(firstStep is VariableStepWigData)
        assertTrue(lastStep is VariableStepWigData)

        assertEquals(position1, firstStep.header.start + 1)
        assertEquals(value1, firstStep.values.first())
        assertEquals(position2, lastStep.header.end)
        assertEquals(value2, lastStep.values.last())
    }

    private fun assertFixedStep(firstStep: WigData, lastStep: WigData,
                                position1: Int, value1: Float,
                                position2: Int, value2: Float) {
        assertTrue(firstStep is FixedStepWigData)
        assertTrue(lastStep is FixedStepWigData)

        assertEquals(position1, firstStep.header.start + 1)
        assertEquals(value1, firstStep.values.first())
        assertEquals(position2, lastStep.header.end)
        assertEquals(value2, lastStep.values.last())
    }
}
