package org.jbb.big;

import java.io.Closeable;
import java.io.DataInput;
import java.io.FilterInputStream;

/**
 * Wrapper for big/little endian DataInputStream
 *
 * @author Sergey Zherevchuk
 */
public class DataInputStreamWrapper<T extends FilterInputStream & DataInput & Closeable>
    implements AutoCloseable {

  private final T wrapped;

  public DataInputStreamWrapper(T wrapped) {
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