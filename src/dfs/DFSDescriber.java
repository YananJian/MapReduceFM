package dfs;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

/**
 * Describe information about the DFS
 * @author Yanan Jian
 * @author Erdong Li
 */
public class DFSDescriber
{
  private String registryHost;
  private int registryPort;

  /**
   * Constructor
   * @param registryHost registry's host
   * @param registryPort registry's port number
   */
  public DFSDescriber(String registryHost, int registryPort)
  {
    this.registryHost = registryHost;
    this.registryPort = registryPort;
  }

  /**
   * Retrieve information about the DFS through RMI call
   * @return string in human-readable format representing the information
   */
  public String describe() throws NotBoundException, RemoteException
  {
    Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
    NameNode namenode = (NameNode) registry.lookup("NameNode");
    return namenode.describeDFS();
  }

  /**
   * Main method of DFSDescriber
   * @param args command-line arguments
   */
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
