package org.jbb.big;

/**
 * B+ tree external node structure
 *
 * @author Sergey Zherevchuk
 */
public class BptNodeLeaf {

  public String key;
  public final int id;
  public final int size;

  public BptNodeLeaf(String key, final int id, final int size) {
    this.key = key;
    this.id = id;
    this.size = size;
  }

}
