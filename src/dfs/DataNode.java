package dfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNode extends Remote
{
  public void putBlock(int blockId, String content) throws RemoteException;
  public String getBlock(int blockId) throws RemoteException;
  public String getDir() throws RemoteException;
  public void heartBeat() throws RemoteException;
  public void terminate() throws RemoteException;
}
