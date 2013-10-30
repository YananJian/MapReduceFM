package dfs;

public class HeartBeatACK implements Message
{
  private static final long serialVersionUID = 4l;

  private int dataNodeId;

  public HeartBeatACK(int dataNodeId)
  {
    this.dataNodeId = dataNodeId;
  }

  public int getDataNodeId()
    { return dataNodeId; }
}
