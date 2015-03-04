package org.jbb.big;

import java.io.Closeable;
import java.io.DataInput;

/**
 * A wrapper for direct and byte swapped {@link java.io.DataInput}.
 *
 * @author Sergey Zherevchuk
 */
public class DataInputStreamWrapper<T extends DataInput & Closeable> implements AutoCloseable {
  private final T wrapped;

  public DataInputStreamWrapper(final T wrapped) {
    this.wrapped = wrapped;
  }

  T getStream() {
    return wrapped;
  }

  @Override
  public void close() throws Exception {
    wrapped.close();
  }
}