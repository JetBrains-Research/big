package org.jbb.big;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * @author Konstantin Kolosovsky.
 */
public class FixedStepWigData extends WigData {

  @NotNull public final float[] values;

  protected FixedStepWigData(@NotNull final WigSectionHeader header) {
    super(header);

    values = new float[header.count];
  }

  public void set(final int index, final float value) {
    values[index] = value;
  }

  @NotNull
  public static FixedStepWigData read(@NotNull final WigSectionHeader header,
                                      @NotNull final SeekableDataInput input) throws IOException {
    final FixedStepWigData result = new FixedStepWigData(header);

    for (int i = 0; i < header.count; i++) {
      final float value = input.readFloat();

      result.set(i, value);
    }

    return result;
  }
}
