package dfs;

import java.util.List;
import java.util.LinkedList;

/**
 * Block info record
 * @author Yanan Jian
 * @author Erdong Li
 */
public class BlockInfo
{
  private int blockId;
  private String filename;
  private List<Integer> dataNodeIds;

  /**
   * Constructor
   * @param blockId block id
   * @param filename filename in dfs
   */
  public BlockInfo(int blockId, String filename)
  {
    this.blockId = blockId;
    this.filename = filename;
    dataNodeIds = new LinkedList<Integer>();
  }

  /**
   * Get the block ID
   * @return block id
   */
  public int getBlockId()
    { return blockId; }

  /**
   * Get the filename of the file this block belongs to
   * @return filename
   */
  public String getFileName()
    { return filename; }

  /**
   * Get IDs of all DataNode that have this block
   * @return datanode ids
   */
  public List<Integer> getDataNodeIds()
    { return dataNodeIds; }

  /**
   * Add a DataNode ID to the list
   * @param dataNodeId datanode id
   */
  public void addDataNode(int dataNodeId)
    { dataNodeIds.add(dataNodeId); }

  /**
   * Translate block info to fsImage format
   * @return string representint the info in fsImage format
   */
  public String toFsImage()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(blockId + " ");
    sb.append(filename + " ");
    for (Integer dataNodeId : dataNodeIds)
      sb.append(dataNodeId + " ");
    return sb.toString();
  }

  /**
   * Translate block info to human-readable string
   * @return human-readable string representing the info
   */
  @Override
  public String toString()
    { return "block id: " + blockId + "\tfilename: " + filename + "\tdatanode id: " + dataNodeIds.toString(); }
}
