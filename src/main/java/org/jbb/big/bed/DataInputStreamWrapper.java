package org.jbb.big.bed;

import java.io.Closeable;
import java.io.DataInput;

/**
 * Wrapper for big/little endian DataInputStream
 *
 * @author Sergey Zherevchuk
 */
public class DataInputStreamWrapper<T extends DataInput & Closeable> implements AutoCloseable {
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