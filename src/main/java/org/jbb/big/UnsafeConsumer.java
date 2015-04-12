package org.jbb.big;

import java.io.IOException;

/**
 * An impure variant of {@link java.util.function.Consumer} which might
 * throw an {@link java.io.IOException}.
 *
 * @author Sergei Lebedev
 * @date 12/04/15
 */
public interface UnsafeConsumer<T> {
  void consume(T value) throws IOException;
}
