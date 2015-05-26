package org.jbb.big;

public class rTree {
  rTree next;	/* Next on same level. */
  rTree children;	/* Child list. */
  rTree parent;	/* Our parent if any. */
  int startChromIx;	/* Starting chromosome. */
  int startBase;		/* Starting base position. */
  int endChromIx;		/* Ending chromosome. */
  int endBase;		/* Ending base. */
  long startFileOffset;	/* Start offset in file for leaves. */
  long endFileOffset;	/* End file offset for leaves. */

  public rTree(){

  }
  public rTree(final rTree other) {
    next = other.next;
    children = other.children;
    parent = other.parent;
    startChromIx = other.startChromIx;
    startBase = other.startBase;
    endChromIx = other.endChromIx;
    endBase = other.endBase;
    startFileOffset = other.startFileOffset;
    endFileOffset = other.endFileOffset;
  }
}
