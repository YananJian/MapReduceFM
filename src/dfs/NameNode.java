package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NameNode extends Remote
{
  public void register(int id, DataNode datanode) throws RemoteException;
  public void createFile(String filename, int nReplicas) throws RemoteException;
  public BlockInfo allocateBlock(String filename) throws RemoteException;
  public DataNode getDataNode(int id) throws RemoteException;
}
