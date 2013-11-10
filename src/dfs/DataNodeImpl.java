package dfs;

import java.lang.Thread;
import java.lang.InterruptedException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

public class DataNodeImpl implements DataNode
{
  private int id;
  private String dir;
  private String registryHost;
  private int registryPort;
  private List<Integer> blockIds;

  public DataNodeImpl(int id, String dir, String registryHost, int registryPort)
  {
    this.id = id;
    this.dir = dir;
    this.registryHost = registryHost;
    this.registryPort = registryPort;
    blockIds = new LinkedList<Integer>();
  }

  public int getId() throws RemoteException
    { return this.id; }

  public void putBlock(int blockId, String content) throws RemoteException
  {
    blockIds.add(blockId);
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(dir + blockId));
      bw.write(content);
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String getBlock(int blockId) throws RemoteException
  {
    try {
      BufferedReader br = new BufferedReader(new FileReader(dir + blockId));
      StringBuilder content = new StringBuilder();
      String line = null;
      while ((line = br.readLine()) != null)
        content.append(line);
      br.close();
      return content.toString();
    } catch (Exception e) {
      throw new RemoteException("getBlock:", e);
    }
  }

  public String getDir() throws RemoteException
    { return dir; }

  public void heartBeat() throws RemoteException
    {}

  public void terminate() throws RemoteException
    { UnicastRemoteObject.unexportObject(this, true); }

  public void bootstrap(int port)
  {
    /* init dir */
    File folder = new File(dir);
    if (!folder.exists())
      folder.mkdirs();
    /* export to runtime */
    DataNode stub = null;
    try {
      stub = (DataNode) UnicastRemoteObject.exportObject(this, port);
    } catch (RemoteException e) {
      e.printStackTrace();
      System.exit(1);
    }
    /* register to namenode */
    while (true) {
      try {
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        NameNode namenode = (NameNode) registry.lookup("NameNode");
        namenode.register(id, stub);
        System.out.println("Registered");
        break;
      } catch (Exception e) {
        /* NameNode not ready, keep trying */
        e.printStackTrace();
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void main(String[] args)
  {
    int id = Integer.parseInt(args[0]);
    String dir = args[1];
    int port = Integer.parseInt(args[2]);
    String registryHost = args[3];
    int registryPort = Integer.parseInt(args[4]);
    DataNodeImpl datanode = new DataNodeImpl(id, dir, registryHost, registryPort);
    datanode.bootstrap(port);
  }
}
