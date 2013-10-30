package dfs;

public class AllocateBlockRequest implements Message
{
  private static final long serialVersionUID = 6l;

  private String filename;

  public AllocateBlockRequest(String filename)
    { this.filename = filename; }

  public String getFileName()
    { return filename; }
}
