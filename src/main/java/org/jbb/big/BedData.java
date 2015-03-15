package org.jbb.big;

/**
 * Created by pacahon on 14.03.15.
 */
public class BedData {

  public final int id;
  public final int start;
  public final int end;
  public final String rest;

  public BedData(int id, int start, int end, String rest) {
    this.id = id;
    this.start = start;
    this.end = end;
    this.rest = rest;
  }
}
