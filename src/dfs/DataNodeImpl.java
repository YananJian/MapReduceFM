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
    this.registryHost = registryHost;
    this.registryPort = registryPort;
    this.dir = dir;
    blockIds = new LinkedList<Integer>();
  }

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

  public void bootstrap()
  {
    /* get all blocks */
    File folder = new File(dir);
    if (!folder.exists())
      folder.mkdirs();
    Registry registry = null;
    DataNode stub = null;
    try {
      stub = (DataNode) UnicastRemoteObject.exportObject(this, 0);
    } catch (RemoteException e) {
      e.printStackTrace();
      System.exit(1);
    }
    /* rebind to registry */
    while (true) {
      System.out.println("Trying to register self");
      try {
        registry = LocateRegistry.getRegistry(registryHost, registryPort);
        registry.rebind(Integer.toString(id), stub);
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
    /* register to namenode */
    while (true) {
      try {
        NameNode namenode = (NameNode) registry.lookup("NameNode");
        if (namenode == null) {
          System.out.println("Oops");
          System.out.println(registryHost + registryPort);
        }
        namenode.register(id, this);
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
    String registryHost = args[2];
    int registryPort = Integer.parseInt(args[3]);
    DataNodeImpl datanode = new DataNodeImpl(id, dir, registryHost, registryPort);
    datanode.bootstrap();
  }
}
