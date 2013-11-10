package dfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

public class DFSTerminator
{
  private String registryHost;
  private int registryPort;
  private String fsImageDir;

  public DFSTerminator(String registryHost, int registryPort, String fsImageDir)
  {
    this.registryHost = registryHost;
    this.registryPort = registryPort;
    this.fsImageDir = fsImageDir;
  }

  public void terminate() throws NotBoundException, RemoteException
  {
    Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
    NameNode namenode = (NameNode) registry.lookup("NameNode");
    namenode.terminate(fsImageDir);
  }

  public static void main(String[] args)
  {
    String registryHost = args[0];
    int registryPort = Integer.parseInt(args[1]);
    String fsImageDir = args[2];
    DFSTerminator terminator = new DFSTerminator(registryHost, registryPort, fsImageDir);
    try {
      terminator.terminate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
