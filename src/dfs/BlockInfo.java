package dfs;

import java.util.LinkedList;

/**
 * Block info record
 * @author Yanan Jian
 * @author Erdong Li
 */
public class BlockInfo
{
  private int blockId;
  private LinkedList<Integer> dataNodeIds;

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
  public LinkedList<Integer> getDataNodeIds()
    { return dataNodeIds; }

  /**
   * Add a datanode id to the list
   * @param dataNodeId datanode id
   */
  public void addDataNode(int dataNodeId)
    { dataNodeIds.add(dataNodeId); }
}
