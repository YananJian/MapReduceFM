package dfs;

public class GetDataNodeInfoResponse implements Message
{
  private static final long serialVersionUID = 7l;

  private DataNodeInfo dataNodeInfo;

  public GetDataNodeInfoResponse(DataNodeInfo dataNodeInfo)
    { this.dataNodeInfo = dataNodeInfo; }

  public DataNodeInfo getDataNodeInfo()
    { return dataNodeInfo; }
}
