package dfs;

import java.lang.Thread;
import java.lang.InterruptedException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
  private LinkedList<Integer> blockIds;

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
    File block = new File(dir + blockId);
    try {
      block.createNewFile();
      BufferedWriter bw = new BufferedWriter(new FileWriter(block.getAbsoluteFile()));
      bw.write(content);
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void heartBeat() throws RemoteException
    {}

  public void bootstrap()
  {
    while (true) {
      System.out.println("Trying to register self");
      try {
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        DataNode stub = (DataNode) UnicastRemoteObject.exportObject(this, 0);
        registry.rebind(Integer.toString(id), stub);
        NameNode namenode = (NameNode) registry.lookup("NameNode");
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
