package org.jbb.big;

import junit.framework.TestCase;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

/**
 * @author Konstantin Kolosovsky.
 */
public class BigWigTest extends TestCase {

  public void testCompressedExample2() throws Exception {
    assertVariableStep(Examples.get("example2.bw"), "chr21", 9411191, 50f, 48119895, 60f);
  }

  public void testVariableStep() throws Exception {
    assertVariableStep(Examples.get("variable_step.bw"), "chr2", 300701, 12.5f, 300705, 12.5f);
  }

  public void testVariableStepWithSpan() throws Exception {
    assertVariableStep(Examples.get("variable_step_with_span.bw"), "chr2", 300701, 12.5f, 300705,
                       12.5f);
  }

  public void testFixedStep() throws Exception {
    assertFixedStep(Examples.get("fixed_step.bw"), "chr3", 400601, 11f, 400801, 33f);
  }

  public void testFixedStepWithSpan() throws Exception {
    assertFixedStep(Examples.get("fixed_step_with_span.bw"), "chr3", 400601, 11f, 400805, 33f);
  }

  private void assertVariableStep(@NotNull final Path path, @NotNull final String chromosome,
                                  final int position1, final float value1, final int position2,
                                  final float value2) throws IOException {
    final List<WigData> steps = assertChromosome(path, chromosome);

    assertVariableStep(steps.get(0), steps.get(steps.size() - 1), position1, value1,
                       position2, value2);
  }

  private void assertFixedStep(@NotNull final Path path, @NotNull final String chromosome,
                               final int position1, final float value1, final int position2,
                               final float value2) throws IOException {
    final List<WigData> steps = assertChromosome(path, chromosome);

    assertFixedStep(steps.get(0), steps.get(steps.size() - 1), position1, value1,
                    position2, value2);
  }

  @NotNull
  private List<WigData> assertChromosome(@NotNull final Path path, @NotNull final String chromosome)
      throws IOException {
    final BigWigFile file = BigWigFile.parse(path);
    final Set<String> chromosomes = file.chromosomes();

    assertEquals(1, chromosomes.size());
    assertEquals(chromosome, chromosomes.iterator().next());

    final List<WigData> steps = file.query(chromosome, 0, 0);

    assertTrue(steps.size() > 0);

    return steps;
  }

  private void assertVariableStep(@NotNull final WigData firstStep, @NotNull final WigData lastStep,
                                  final int position1, final float value1, final int position2,
                                  final float value2) {
    assertTrue(firstStep instanceof VariableStepWigData);
    assertTrue(lastStep instanceof VariableStepWigData);

    assertEquals(position1, firstStep.header.start + 1);
    assertEquals(value1, ((VariableStepWigData) firstStep).values[0]);
    assertEquals(position2, lastStep.header.end);
    assertEquals(value2, ((VariableStepWigData) lastStep).values[lastStep.header.count - 1]);
  }

  private void assertFixedStep(@NotNull final WigData firstStep, @NotNull final WigData lastStep,
                               final int position1, final float value1, final int position2,
                               final float value2) {
    assertTrue(firstStep instanceof FixedStepWigData);
    assertTrue(lastStep instanceof FixedStepWigData);

    assertEquals(position1, firstStep.header.start + 1);
    assertEquals(value1, ((FixedStepWigData) firstStep).values[0]);
    assertEquals(position2, lastStep.header.end);
    assertEquals(value2, ((FixedStepWigData) lastStep).values[lastStep.header.count - 1]);
  }
}
