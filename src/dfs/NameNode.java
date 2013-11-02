package dfs;

import java.util.List;
import java.util.Map;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NameNode extends Remote
{
  public void register(int id, DataNode datanode, List<Integer> blockIds) throws RemoteException;
  public void createFile(String filename, int nReplicas) throws RemoteException;
  public int getBlockSize() throws RemoteException;
  public int getNextBlockId() throws RemoteException;
  public int allocateBlock() throws RemoteException;
  public void commitBlockAllocation(int dataNodeId, int blockId) throws RemoteException;
  public Map<Integer, List<Integer>> getAllBlocks(String filename) throws RemoteException;
  public String describeDFS() throws RemoteException;
}
