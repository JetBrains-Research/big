package org.jbb.big;

import java.io.Closeable;
import java.io.DataInput;
import java.io.FilterInputStream;

/**
 * A wrapper for direct and byte swapped {@link java.io.DataInput}.
 *
 * @author Sergey Zherevchuk
 */
public class DataInputStreamWrapper<T extends FilterInputStream & DataInput & Closeable>
    implements AutoCloseable {

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