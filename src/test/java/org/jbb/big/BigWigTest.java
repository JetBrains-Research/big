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

  public void testVariableStepParse() throws Exception {
    assertExample2(Examples.get("example2_uncompressed.bw"));
  }

  public void testCompressedVariableStepParse() throws Exception {
    assertExample2(Examples.get("example2.bw"));
  }

  private void assertExample2(@NotNull final Path path) throws IOException {
    final BigWigFile file = BigWigFile.parse(path);
    final Set<String> chromosomes = file.chromosomes();

    assertEquals(1, chromosomes.size());
    assertEquals("chr21", chromosomes.iterator().next());

    final List<WigData> steps = file.query("chr21", 0, 0);

    assertTrue(steps.size() > 0);

    final WigData first = steps.get(0);
    final WigData last = steps.get(steps.size() - 1);

    assertTrue(first instanceof VariableStepWigData);
    assertTrue(last instanceof VariableStepWigData);

    assertEquals(9411191, first.header.start + 1);
    assertEquals(50f, ((VariableStepWigData) first).values[0]);
    assertEquals(48119891, ((VariableStepWigData) last).positions[last.header.count - 1] + 1);
    assertEquals(60f, ((VariableStepWigData) last).values[last.header.count - 1]);
  }
}
