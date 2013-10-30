package dfs;

public class PutBlockRequest implements Message
{
  private static final long serialVersionUID = 3l;

  private int blockId;
  private String content;

  public PutBlockRequest(int blockId, String content)
  {
    this.blockId = blockId;
    this.content = content;
  }

  public int getBlockId()
    { return blockId; }

  public String getContent()
    { return content; }
}
