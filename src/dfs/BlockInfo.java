package dfs;

import java.util.List;
import java.util.LinkedList;

/**
 * Block info record
 * @author Yanan Jian
 * @author Erdong Li
 */
public class BlockInfo
{
  private int blockId;
  private List<Integer> dataNodeIds;

  /**
   * Constructor
   * @param blockId block id
   */
  public BlockInfo(int blockId)
  {
    this.blockId = blockId;
    dataNodeIds = new LinkedList<Integer>();
  }

  /**
   * Get the block id
   * @return block id
   */
  public int getBlockId()
    { return blockId; }

  /**
   * Get ids of all data node that have this block
   * @return datanode ids
   */
  public List<Integer> getDataNodeIds()
    { return dataNodeIds; }

  /**
   * Add a datanode id to the list
   * @param dataNodeId datanode id
   */
  public void addDataNode(int dataNodeId)
    { dataNodeIds.add(dataNodeId); }

  @Override
  public String toString()
    { return "block id: " + blockId + "\tdatanode id: " + dataNodeIds.toString(); }
}
