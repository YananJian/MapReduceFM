package dfs;

import java.util.List;
import java.util.LinkedList;

/**
 * File info record
 * @author Yanan Jian
 * @author Erdong Li
 */
public class FileInfo
{
  private String filename;
  private int nReplicas;
  private List<Integer> blockIds;

  /**
   * Constructor
   * @param filename file name
   * @param nReplicas replication factor
   */
  public FileInfo(String filename, int nReplicas)
  {
    this.filename = filename;
    this.nReplicas = nReplicas;
    blockIds = new LinkedList<Integer>();
  }

  /**
   * Get the filename
   * @return filename
   */
  public String getFileName()
    { return filename; }

  /**
   * Get the replication factor
   * @return replication factor
   */
  public int getNReplicas()
    { return nReplicas; }

  /**
   * Get the block IDs
   * @return block ids
   */
  public List<Integer> getBlockIds()
    { return blockIds; }

  /**
   * Add a block ID to the list
   * @param blockId block id
   */
  public void addBlockId(int blockId)
    { blockIds.add(blockId); }

  /**
   * Translate file info to fsImage format
   * @return string representint the info in fsImage format
   */
  public String toFsImage()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(filename + " ");
    sb.append(nReplicas + " ");
    for (int blockId : blockIds)
      sb.append(blockId + " ");
    return sb.toString();
  }

  /**
   * Translate file info to human-readable string
   * @return human-readable string representing the info
   */
  @Override
  public String toString()
    { return "filename: " + filename + "\t#replicas: " + nReplicas + "\tblock id: " + blockIds.toString(); }
}
