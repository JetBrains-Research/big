package org.jbb.big;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * @author Konstantin Kolosovsky.
 */
public class VariableStepWigData extends WigData {

  /**
   * NOTE: Currently all positions are excluded - +1 should be added to get correct position (for
   * instance to convert from bigWig to wig format). It is raw representation of bigWig data -
   * probably such representation will give some other bonuses. If not this representation could be
   * revised.
   */
  public final int[] positions;
  public final float[] values;

  protected VariableStepWigData(@NotNull final WigSectionHeader header) {
    super(header);

    positions = new int[header.count];
    values = new float[header.count];
  }

  public void set(final int index, final int position, final float value) {
    positions[index] = position;
    values[index] = value;
  }

  @NotNull
  public static VariableStepWigData read(@NotNull final WigSectionHeader header,
                                         @NotNull final SeekableDataInput input)
      throws IOException {
    final VariableStepWigData result = new VariableStepWigData(header);

    for (int i = 0; i < header.count; i++) {
      final int position = input.readInt();
      final float value = input.readFloat();

      result.set(i, position, value);
    }

    return result;
  }
}
