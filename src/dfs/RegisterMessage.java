package dfs;

import java.util.LinkedList;

public class RegisterMessage implements Message
{
  private static final long serialVersionUID = 0l;
  int dataNodeId;
  LinkedList<Integer> blockIds;

  public RegisterMessage(int dataNodeId, LinkedList<Integer> blockIds)
  {
    this.dataNodeId = dataNodeId;
    this.blockIds = blockIds;
  }

  public int getDataNodeId()
    { return dataNodeId; }

  public LinkedList<Integer> getBlockIds()
    { return blockIds; }
}
