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

  private int dataNodeId;
  private String host;
  private int port;
  private boolean alive;

  /**
   * Constructor
   * @param dataNodeId datanode id
   * @param host host
   * @param port port number
   */
  public DataNodeInfo(int dataNodeId, String host, int port)
  {
    this.dataNodeId = dataNodeId;
    this.host = host;
    this.port = port;
    alive = false;
  }

  public int getDataNodeId()
    { return dataNodeId; }

  public String getHost()
    { return host; }

  public int getPort()
    { return port; }

  public boolean isAlive()
    { return alive; }

  public void setAlive(boolean alive)
    { this.alive = alive; }
}
