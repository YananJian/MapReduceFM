package dfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

public class DFSDescriber
{
  private String registryHost;
  private int registryPort;

  public DFSDescriber(String registryHost, int registryPort)
  {
    this.registryHost = registryHost;
    this.registryPort = registryPort;
  }

  public String describe() throws NotBoundException, RemoteException
  {
    Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
    NameNode namenode = (NameNode) registry.lookup("NameNode");
    return namenode.describeDFS();
  }

  public static void main(String[] args)
  {
    String registryHost = args[0];
    int registryPort = Integer.parseInt(args[1]);
    DFSDescriber describer = new DFSDescriber(registryHost, registryPort);
    try {
      System.out.println(describer.describe());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
