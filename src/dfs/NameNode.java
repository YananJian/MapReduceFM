package dfs;

import java.util.List;
import java.util.Map;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Remote interface for NameNode
 * @author Yanan Jian
 * @author Erdong Li
 */
public interface NameNode extends Remote
{
  /**
   * Register a DataNode to this NameNode
   * @param id datanode;s id
   * @param datanode stub of datanode
   * @throws RemoteException
   */
  public void register(int id, DataNode datanode) throws RemoteException;

  /**
   * Retrieve a stub of DataNode with given ID
   * @return stub of datanode
   * @throws RemoteException
   */
  public DataNode getDataNode(int id) throws RemoteException;

  /**
   * Create metadata for given filename
   * @param filename filename
   * @param nReplicas requested replication factor
   * @return actual replication factor
   * @throws RemoteException
   */
  public int createFile(String filename, int nReplicas) throws RemoteException;

  /**
   * Retrieve the default block size
   * @return default block size
   * @throws RemoteException
   */
  public int getBlockSize() throws RemoteException;

  /**
   * Retrieve block ID for the next block
   * @param filename filename for the block
   * @return block id for the next block
   * @throws RemoteException
   */
  public int getNextBlockId(String filename) throws RemoteException;

  /**
   * Retrieve the next DataNode for storing the replica
   * @return the next datanode chosen
   * @throws RemoteException
   */
  public DataNode allocateBlock() throws RemoteException;

  /**
   * Commit the block allocation
   * @param dataNodeId datanode id that sotres the block
   * @param filename filename of the block
   * @param blockId block id
   * @throws RemoteException
   */
  public void commitBlockAllocation(int dataNodeId, String filename, int blockId) throws RemoteException;

  /**
   * Retrieve all block IDs with their DataNode IDs for the file with given filename
   * @param filename filename of the blocks
   * @return a map from block id to datanode ids
   * @throws RemoteException
   */
  public Map<Integer, List<Integer>> getAllBlocks(String filename) throws RemoteException;

  /**
   * Describe information of the DFS
   * @return human-readable string representing the dfs
   * @throws RemoteException
   */
  public String describeDFS() throws RemoteException;

  /**
   * Terminate all nodes, and write metadata to fsImage
   * @param fsImageDir directory to fsImage
   * @throws RemoteException
   */
  public void terminate(String fsImageDir) throws RemoteException;
}
