package dfs;

import java.io.Serializable;

/**
 * DataNode info record
 * @author Yanan Jian
 * @author Erdong Li
 */
public class DataNodeInfo implements Serializable
{
  private static final long serialVersionUID = 2l;

  private int id;
  private DataNode datanode;
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
    alive = true;
  }

  public int getId()
    { return id; }

  public DataNode getDataNode()
    { return datanode; }

  public boolean isAlive()
    { return alive; }

  public void setAlive(boolean alive)
    { this.alive = alive; }
}
