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

  public int getId()
    { return id; }

  public void setDataNode(DataNode datanode)
    { this.datanode = datanode; }

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

  public String toFsImage()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(id + " ");
    for (int blockId : blockIds)
      sb.append(blockId + " ");
    return sb.toString();
  }

  @Override
  public int compareTo(DataNodeInfo other)
    { return blockIds.size() - other.blockIds.size(); }

  @Override
  public String toString()
    { return "datanode id: " + id + "\tblock id: " + blockIds.toString() + "\t alive: " + alive; }
}
