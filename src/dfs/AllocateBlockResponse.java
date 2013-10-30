package dfs;

public class AllocateBlockResponse implements Message
{
  private static final long serialVersionUID = 7l;

  private BlockInfo bi;

  public AllocateBlockResponse(BlockInfo bi)
    { this.bi = bi; }

  public BlockInfo getBlockInfo()
    { return bi; } 
}
