package org.jbb.big;

/**
 * Created by slipstak2 on 01.05.15.
 */
public class cirTreeRange {
  /* A chromosome id and an interval inside it. */

    int chromIx;	/* Chromosome id. */
    int start;	/* Start position in chromosome. */
    int end;		/* One past last base in interval in chromosome. */
  public cirTreeRange(){

  }

  @Override
  public String toString() {
    return String.format("%d; %d; %d", chromIx, start, end);
  }
}
