package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Remote interface for DataNode
 * @author Yanan Jian
 * @author Erdong Li
 */
public interface DataNode extends Remote
{
  /**
   * Get DataNode's ID
   * @return datanode's id
   * @throws RemoteException
   */
  public int getId() throws RemoteException;

  /**
   * Put a block with blockId and given content to the DataNode
   * @param blockId block's id
   * @param content content in string format
   * @throws RemoteException
   */
  public void putBlock(int blockId, String content) throws RemoteException;

  /**
   * Put a block with blockId and given content to the DataNode
   * @param blockId block's id
   * @param content content in byte format
   * @throws RemoteException
   */
  public void putBlock(int blockId, byte[] content) throws RemoteException;

  /**
   * Retrieve a block with blockId and given content to the DataNode directly from another DataNode
   * @param datanode the sender
   * @param blockId block's id
   * @throws RemoteException
   */
  public void putBlock(DataNode datanode, int blockId) throws RemoteException;

  /**
   * Retrieve content from block with given block ID
   * @param blockId block's id
   * @return block content in string format
   * @throws RemoteException
   */
  public String getBlock(int blockId) throws RemoteException;

  /**
   * Retrieve content from block with given block ID in byte
   * @param blockId block's id
   * @return block content in string format
   * @throws RemoteException
   */
  public byte[] getByteBlock(int blockId) throws RemoteException;

  /**
   * Get directory for data blocks
   * @return directory for data blocks
   * @throws RemoteException
   */
  public String getDir() throws RemoteException;

  /**
   * Heat beat method for health check
   * @throws RemoteException;
   */
  public void heartBeat() throws RemoteException;

  /**
   * Terminate the DataNode
   * @throws RemoteException
   */
  public void terminate() throws RemoteException;
}
