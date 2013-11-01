package dfs;

import java.lang.Thread;
import java.util.Hashtable;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

public class NameNodeImpl implements NameNode
{
  private int nReplicasDefault;
  private int healthCheckInterval;
  private int counter;
  private Integer blockCounter;
  private Registry registry;
  private Hashtable<String, FileInfo> fileInfos;
  private Hashtable<Integer, BlockInfo> blockInfos;
  private Hashtable<Integer, DataNodeInfo> dataNodeInfos;

  /**
   * Constructor
   * @param port port of NameNode
   */
  public NameNodeImpl(int nReplicasDefault, int port, int healthCheckInterval)
  {
    this.nReplicasDefault = nReplicasDefault;
    this.healthCheckInterval = healthCheckInterval;
    counter = 0;
    blockCounter = 0;
    try {
      registry = LocateRegistry.createRegistry(port);
    } catch (RemoteException e) {
      e.printStackTrace();
      System.exit(1);
    }
    fileInfos = new Hashtable<String, FileInfo>();
    blockInfos = new Hashtable<Integer, BlockInfo>();
    dataNodeInfos = new Hashtable<Integer, DataNodeInfo>();
  }

  public void register(int id, DataNode datanode) throws RemoteException
    { dataNodeInfos.put(id, new DataNodeInfo(id, datanode)); }

  public void createFile(String filename, int nReplicas) throws RemoteException
  {
    if (nReplicas == 0)
      nReplicas = nReplicasDefault;
    fileInfos.put(filename, new FileInfo(filename, nReplicas));
  }

  public BlockInfo allocateBlock(String filename) throws RemoteException
  {
    FileInfo fi = fileInfos.get(filename);
    int nReplicas = fi.getNReplicas();
    int blockId = 0;
    synchronized(blockCounter) {
      blockId = blockCounter++;
    }
    BlockInfo bi = new BlockInfo(blockId);
    fi.addBlockId(blockId);
    /* select DataNode using round-robin policy */
    for (int i = 0; i < nReplicas;) {
      int dataNodeId = counter++ % dataNodeInfos.size();
      if (dataNodeInfos.get(dataNodeId).isAlive()) {
        bi.addDataNode(dataNodeId);
        counter++;
      }
    }
    blockInfos.put(blockId, bi);
    return bi;
  }

  public DataNode getDataNode(int id) throws RemoteException
    { return dataNodeInfos.get(id).getDataNode(); }

  public void addDataNode(int id, DataNode datanode)
    { dataNodeInfos.put(id, new DataNodeInfo(id, datanode)); }

  public void bootstrap(int nDataNodes)
  {
    /* register itself to registry */
    try {
      NameNode stub = (NameNode) UnicastRemoteObject.exportObject(this, 0);
      registry.rebind("NameNode", stub);
    } catch (RemoteException e) {
      e.printStackTrace();
      System.exit(1);
    }

    /* wait for each datanode to get online */
    while (dataNodeInfos.size() < nDataNodes) {
      System.out.println("#Registered DataNodes: " + dataNodeInfos.size() + "/" + nDataNodes);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void healthCheck()
  {
    /* start healthcheck */
    while (true) {
      try {
        Thread.sleep(healthCheckInterval);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      for (int i = 0; i < dataNodeInfos.size(); i++) {
        DataNodeInfo dni = dataNodeInfos.get(i);
        if (dni.isAlive()) {
          /* ignore dead nodes */
          try {
            dni.getDataNode().heartBeat();
          } catch (RemoteException e) {
            /* unreachable */
            System.out.println("DataNode #" + i + " is dead");
            dni.setAlive(false);
          }
        }
      }
    }
  }

  public static void main(String[] args)
  {
    int port = Integer.parseInt(args[0]);
    int nReplicasDefault = Integer.parseInt(args[1]);
    int nDataNodes = Integer.parseInt(args[2]);
    int healthCheckInterval = Integer.parseInt(args[3]);

    NameNodeImpl namenode = new NameNodeImpl(nReplicasDefault, port, healthCheckInterval);

    /* bootstrap */
    namenode.bootstrap(nDataNodes);

    /* register to registry and start health check */
    namenode.healthCheck();
  }
}
