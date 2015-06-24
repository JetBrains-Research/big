package org.jbb.big;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Various internal helpers.
 *
 * You shouldn't be using them outside of {@code big}.
 *
 * @author Sergei Lebedev
 * @since 24/06/15
 */
class Internals {
  /**
   * Reads chromosome sizes from a tab-delimited two-column file.
   */
  public static Map<String, Integer> readChromosomeSizes(final Path path) throws IOException {
    return Files.lines(path)
        .map(line -> line.split("\t", 2))
        .collect(Collectors.toMap(chunks -> chunks[0],
                                  chunks -> Integer.parseInt(chunks[1])));
  }
}
