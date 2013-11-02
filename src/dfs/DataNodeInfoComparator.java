package dfs;

import java.util.Comparator;
import java.util.Map;

public class DataNodeInfoComparator implements Comparator<Integer> {
  Map<Integer, DataNodeInfo> base;

  public DataNodeInfoComparator(Map<Integer, DataNodeInfo> base)
    { this.base = base; }

  public int compare(Integer left, Integer right) {
    System.out.println(1);
    if (base.get(left).getNBlocks() >= base.get(right).getNBlocks())
      return -1;
    else
      return 1;
  }
}
