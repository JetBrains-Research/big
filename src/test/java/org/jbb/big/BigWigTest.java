package org.jbb.big;

import junit.framework.TestCase;

import java.util.List;
import java.util.Set;

/**
 * @author Konstantin Kolosovsky.
 */
public class BigWigTest extends TestCase {

  public void testBigWigVariableStepParse() throws Exception {
    final BigWigFile file = BigWigFile.parse(Examples.get("example2_uncompressed.bw"));
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
