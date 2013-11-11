package dfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

/**
 * Terminate the whole DFS
 * @author Yanan Jian
 * @author Erdong Li
 */
public class DFSTerminator
{
  private String registryHost;
  private int registryPort;
  private String fsImageDir;

  /**
   * Constructor
   * @param registryHost registry's host
   * @param registryPort registry's port number
   * @param fsImageDir directory to fsImage
   */
  public DFSTerminator(String registryHost, int registryPort, String fsImageDir)
  {
    this.registryHost = registryHost;
    this.registryPort = registryPort;
    this.fsImageDir = fsImageDir;
  }

  /**
   * Terminate the whole DFS through RMI call
   */
  public void terminate() throws NotBoundException, RemoteException
  {
    Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
    NameNode namenode = (NameNode) registry.lookup("NameNode");
    namenode.terminate(fsImageDir);
  }

  /**
   * Main method of DFSTerminator
   * @param args command-line arguments
   */
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
