package dfs;

import java.util.List;
import java.util.LinkedList;

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
  public DataNodeInfo(int id, DataNode datanode)
  {
    this.id = id;
    this.datanode = datanode;
    this.blockIds = new LinkedList<Integer>();
    this.alive = false;
  }

  /**
   * Get the DataNode's ID
   * @return datanode's id
   */
  public int getId()
    { return id; }

  /**
   * Set DataNode
   * @param datanode datanode stub
   */
  public void setDataNode(DataNode datanode)
    { this.datanode = datanode; }

  /**
   * Get DataNode
   * @return datanode stub
   */
  public DataNode getDataNode()
    { return datanode; }

  /**
   * Add a block ID to the list
   * @param blockId block id
   */
  public void addBlock(int blockId)
    { blockIds.add(blockId); }

  /**
   * Get number of blocks this DataNode have
   * @return number of blocks
   */
  public int getNBlocks()
    { return blockIds.size(); }

  /**
   * Get IDs of all blocks this DataNode have
   * @return block ids
   */
  public List<Integer> getBlockIds()
    { return blockIds; }

  /**
   * Return true if this DataNode is alive
   * @return if the datanode is alive
   */
  public boolean isAlive()
    { return alive; }

  /**
   * Set if this DataNode is alive
   * @param alive boolean representing if the datanode is alive
   */
  public void setAlive(boolean alive)
    { this.alive = alive; }

  /**
   * Translate DataNode info to fsImage format
   * @return string representint the info in fsImage format
   */
  public String toFsImage()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(id + " ");
    for (int blockId : blockIds)
      sb.append(blockId + " ");
    return sb.toString();
  }

  /**
   * Compare two DataNodes based on number of blocks
   * @return difference between numbers of blocks
   */
  @Override
  public int compareTo(DataNodeInfo other)
    { return blockIds.size() - other.blockIds.size(); }

  /**
   * Translate DataNode info to human-readable string
   * @return human-readable string representing the info
   */
  @Override
  public String toString()
    { return "datanode id: " + id + "\tblock id: " + blockIds.toString() + "\t alive: " + alive; }
}
