package org.jbb.big;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * Created by slipstak2 on 01.05.15.
 */
public class myStream {

  ArrayList<Byte> data;

  myStream() {
    data = new ArrayList<>();
  }

  void writeBytes(byte[] bytes) {
    for (byte b : bytes) {
      data.add(b);
    }
  }

  void writeInt(int val) {
    writeBytes(ByteBuffer.allocate(4).putInt(val).array());
  }

  void writeChar(char c) {
    data.add((byte) c);
  }

  void writeString(String s) {
    writeBytes(s.getBytes());
  }

  int size() {
    return data.size();
  }

  void reset() {
    data = new ArrayList<>();
  }

  byte[] toByteArray() {
    byte[] dst = new byte[data.size()];
    for (int i = 0; i < data.size(); ++i) {
      dst[i] = data.get(i);
    }
    return dst;
  }
}


