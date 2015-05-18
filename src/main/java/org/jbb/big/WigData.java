package org.jbb.big;

import org.jetbrains.annotations.NotNull;

/**
 * @author Konstantin Kolosovsky.
 */
public abstract class WigData {

  @NotNull protected final WigSectionHeader header;

  protected WigData(@NotNull final WigSectionHeader header) {
    this.header = header;
  }
}
