package dfs;

/**
 * DataNode info record
 * @author Yanan Jian
 * @author Erdong Li
 */
public class DataNodeInfo
{
  private int id;
  private int nBlocks;
  private DataNode datanode;
  private boolean alive;

  /**
   * Constructor
   * @param id datanode id
   * @param datanode remote object
   */
  public DataNodeInfo(int id, int nBlocks, DataNode datanode)
  {
    this.id = id;
    this.nBlocks = nBlocks;
    this.datanode = datanode;
    alive = true;
  }

  public int getId()
    { return id; }

  public int getNBlocks()
    { return nBlocks; }

  public void setNBlocks(int nBlocks)
    { this.nBlocks = nBlocks; }

  public DataNode getDataNode()
    { return datanode; }

  public boolean isAlive()
    { return alive; }

  public void setAlive(boolean alive)
    { this.alive = alive; }
}
