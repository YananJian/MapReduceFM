package dfs;

public class CreateFileRequest implements Message
{
  private static final long serialVersionUID = 5l;

  private String filename;
  private int nReplicas;

  public CreateFileRequest(String filename, int nReplicas)
  {
    this.filename = filename;
    this.nReplicas = nReplicas;
  }

  public String getFileName()
    { return filename; }

  public int getNReplicas()
    { return nReplicas; }
}
