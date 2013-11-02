package dfs;

import java.lang.Thread;
import java.util.List;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Hashtable;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

public class NameNodeImpl implements NameNode
{
  private int nReplicasDefault;
  private int healthCheckInterval;
  private int blockSize;
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
  public NameNodeImpl(int nReplicasDefault, int healthCheckInterval, int blockSize, int port)
  {
    this.nReplicasDefault = nReplicasDefault;
    this.healthCheckInterval = healthCheckInterval;
    this.blockSize = blockSize;
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

  public int getBlockSize() throws RemoteException
    { return blockSize; }

  public int getNextBlockId() throws RemoteException
  {
    int blockId = 0;
    synchronized(blockCounter) {
      blockId =  blockCounter++;
    }
    blockInfos.put(blockId, new BlockInfo(blockId));
    return blockId;
  }

  public int allocateBlock() throws RemoteException
  {
    /* select DataNode using round-robin policy */
    while (true) {
      int dataNodeId = counter++ % dataNodeInfos.size();
      if (dataNodeInfos.get(dataNodeId).isAlive())
        return dataNodeId;
    }
  }

  public void commitBlockAllocation(int blockId, int dataNodeId) throws RemoteException
    { blockInfos.get(blockId).addDataNode(dataNodeId); }

  public Map<Integer, List<Integer>> getAllBlocks(String filename) throws RemoteException
  {
    FileInfo fi = fileInfos.get(filename);
    Map<Integer, List<Integer>> result = new LinkedHashMap<Integer, List<Integer>>();
    LinkedList<Integer> blockIds = fi.getBlockIds();
    for (int blockId : blockIds)
      result.put(blockId, blockInfos.get(blockId).getDataNodeIds());
    return result;
  }

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
    int nReplicasDefault = Integer.parseInt(args[0]);
    int healthCheckInterval = Integer.parseInt(args[1]);
    int blockSize = Integer.parseInt(args[2]);
    int port = Integer.parseInt(args[3]);
    int nDataNodes = Integer.parseInt(args[4]);

    NameNodeImpl namenode = new NameNodeImpl(nReplicasDefault, healthCheckInterval, blockSize, port);

    /* bootstrap */
    namenode.bootstrap(nDataNodes);

    /* register to registry and start health check */
    namenode.healthCheck();
  }
}
