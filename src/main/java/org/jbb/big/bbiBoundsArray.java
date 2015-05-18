package org.jbb.big;

/**
 * Created by slipstak2 on 30.04.15.
 */
public class bbiBoundsArray {
  long offset;
  cirTreeRange range;
  public bbiBoundsArray(){
    range = new cirTreeRange();
  }

  @Override
  public String toString() {
    return String.format("offset = %d, range = %s", offset, range.toString());
  }
}
