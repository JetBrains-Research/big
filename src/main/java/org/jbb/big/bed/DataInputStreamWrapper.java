package org.jbb.big.bed;

import java.io.Closeable;
import java.io.DataInput;

/**
 * Created by pacahon on 22.02.15.
 */
public class DataInputStreamWrapper<T extends DataInput & Closeable> {
  private final T wrapped;

  public DataInputStreamWrapper(T wrapped) {
    this.wrapped = wrapped;
  }

  T getStream() {
    return wrapped;
  }
}