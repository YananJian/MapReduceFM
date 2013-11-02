package dfs;

import java.util.List;

/**
 * DataNode info record
 * @author Yanan Jian
 * @author Erdong Li
 */
public class DataNodeInfo implements Comparable<DataNodeInfo>
{
  private int id;
  private DataNode datanode;
  private List<Integer> blockIds;
  private boolean alive;

  /**
   * Constructor
   * @param id datanode id
   * @param datanode remote object
   */
  public DataNodeInfo(int id, DataNode datanode, List<Integer> blockIds)
  {
    this.id = id;
    this.datanode = datanode;
    this.blockIds = blockIds;
    alive = true;
  }

  public int getId()
    { return id; }

  public DataNode getDataNode()
    { return datanode; }

  public void addBlock(int blockId)
    { blockIds.add(blockId); }

  public int getNBlocks()
    { return blockIds.size(); }

  public List<Integer> getBlockIds()
    { return blockIds; }

  public boolean isAlive()
    { return alive; }

  public void setAlive(boolean alive)
    { this.alive = alive; }

  @Override
  public int compareTo(DataNodeInfo other)
    { return blockIds.size() - other.blockIds.size(); }

  @Override
  public String toString()
    { return "datanode id: " + id + "\tblock id: " + blockIds.toString() + "\t alive: " + alive; }
}
