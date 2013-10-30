package dfs;

public class GetDataNodeInfoRequest implements Message
{
  private static final long serialVersionUID = 7l;

  private int dataNodeId;

  public GetDataNodeInfoRequest(int dataNodeId)
    { this.dataNodeId = dataNodeId; }

  public int getDataNodeId()
    { return dataNodeId; }
}
