package org.jbb.big;

/**
 * Created by slipstak2 on 30.04.15.
 */
public class bbiChromUsage {

  public String name;	        /* chromosome name. */
  public int itemCount;	/* Number of items for this chromosome. */
  public int id;	        /* Unique ID for chromosome. */
  public int size;	        /* Size of chromosome. */

  public bbiChromUsage(String name, int id, int size){
    this.name = name;
    this.id = id;
    this.size = size;
  }

  @Override
  public String toString() {
    return String.format("%s : %d : %d : %d", name, itemCount, id, size);
  }
}
