package org.jbb.big;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

/**
 * @author Konstantin Kolosovsky.
 */
public class WigSectionHeader {

  public static final byte BED_GRAPH_TYPE = 1;
  public static final byte VARIABLE_STEP_TYPE = 2;
  public static final byte FIXED_STEP_TYPE = 3;

  public final int id;
  /**
   * Start position (exclusive).
   */
  public final int start;
  /**
   * End position (inclusive).
   */
  public final int end;
  public final int step;
  public final int span;
  public final byte type;
  public final byte reserved;
  public final short count;

  public WigSectionHeader(final int id, final int start, final int end, final int step,
                          final int span, final byte type, final byte reserved, final short count) {
    this.id = id;
    this.start = start;
    this.end = end;
    this.step = step;
    this.span = span;
    this.type = type;
    this.reserved = reserved;
    this.count = count;
  }

  @NotNull
  public static WigSectionHeader read(@NotNull final SeekableDataInput input) throws IOException {
    final int id = input.readInt();
    final int start = input.readInt();
    final int end = input.readInt();
    final int step = input.readInt();
    final int span = input.readInt();
    final byte type = input.readByte();
    final byte reserved = input.readByte();
    final short count = input.readShort();

    return new WigSectionHeader(id, start, end, step, span, type, reserved, count);
  }
}
