package dfs;

import java.lang.Thread;
import java.util.Comparator;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Hashtable;
import java.util.SortedSet;
import java.util.TreeSet;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

public class NameNodeImpl implements NameNode
{
  private int nReplicasDefault;
  private int healthCheckInterval;
  private int blockSize;
  private Integer blockCounter;
  private Registry registry;
  private Map<String, FileInfo> fileInfos;
  private Map<Integer, BlockInfo> blockInfos;
  private Map<Integer, DataNodeInfo> dataNodeInfos;

  /**
   * Constructor
   * @param port port of NameNode
   */
  public NameNodeImpl(int nReplicasDefault, int healthCheckInterval, int blockSize, int port)
  {
    this.nReplicasDefault = nReplicasDefault;
    this.healthCheckInterval = healthCheckInterval;
    this.blockSize = blockSize;
    blockCounter = 0;
    try {
      registry = LocateRegistry.createRegistry(port);
    } catch (RemoteException e) {
      e.printStackTrace();
      System.exit(1);
    }
    fileInfos = new Hashtable<String, FileInfo>();
    blockInfos = new Hashtable<Integer, BlockInfo>();
    dataNodeInfos = new TreeMap<Integer, DataNodeInfo>();
  }

  public void register(int id, DataNode datanode, List<Integer> blockIds) throws RemoteException
  {
    dataNodeInfos.put(id, new DataNodeInfo(id, datanode, blockIds));
    for (int blockId : blockIds) {
      if (blockInfos.get(blockId) == null)
        blockInfos.put(blockId, new BlockInfo(blockId));
      if (!blockInfos.get(blockId).getDataNodeIds().contains(id))
        blockInfos.get(blockId).addDataNode(id);
    }
  }

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
    /* select DataNode with least #blocks */
    SortedSet<Map.Entry<Integer, DataNodeInfo>> sortedEntries = getSortedEntries(dataNodeInfos);
    for (Map.Entry<Integer, DataNodeInfo> entry : sortedEntries)
      if (entry.getValue().isAlive())
        return entry.getKey();
    return -1;
  }

  public void commitBlockAllocation(int dataNodeId, int blockId) throws RemoteException
  {
    blockInfos.get(blockId).addDataNode(dataNodeId);
    dataNodeInfos.get(dataNodeId).addBlock(blockId);
  }

  public Map<Integer, List<Integer>> getAllBlocks(String filename) throws RemoteException
  {
    FileInfo fi = fileInfos.get(filename);
    Map<Integer, List<Integer>> result = new LinkedHashMap<Integer, List<Integer>>();
    List<Integer> blockIds = fi.getBlockIds();
    for (int blockId : blockIds)
      result.put(blockId, blockInfos.get(blockId).getDataNodeIds());
    return result;
  }

  public String describeDFS() throws RemoteException
  {
    StringBuilder dfs = new StringBuilder();
    dfs.append("========= dfs info =========\n");
    dfs.append("#default replicas: " + nReplicasDefault + "\n");
    dfs.append("healthcheck interval: " + healthCheckInterval + " second\n");
    dfs.append("block size: " + blockSize + " byte\n");
    dfs.append("========= file info =========\n");
    for (Map.Entry<String, FileInfo> entry : fileInfos.entrySet())
      dfs.append(entry.getValue().toString() + "\n");
    dfs.append("========= block info =========\n");
    for (Map.Entry<Integer, BlockInfo> entry : blockInfos.entrySet())
      dfs.append(entry.getValue().toString() + "\n");
    dfs.append("========= datanode info =========\n");
    for (Map.Entry<Integer, DataNodeInfo> entry : dataNodeInfos.entrySet())
      dfs.append(entry.getValue().toString() + "\n");
    return dfs.toString();
  }

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
        /* ignore dead nodes */
        if (!dni.isAlive())
          continue;
        try {
          dni.getDataNode().heartBeat();
        } catch (RemoteException e) {
          /* unreachable node */
          dni.setAlive(false);
          /* move each replica to other node */
          List<Integer> blockIds = dni.getBlockIds();
          for (int blockId : blockIds) {
            /* get all datanodes that has this block */
            List<Integer> dataNodeIds = blockInfos.get(blockId).getDataNodeIds();
            String replica = null;
            /* assume datanodes that have this replica won't all fail at the same time */
            for (int dataNodeId : dataNodeIds) {
              if (dataNodeInfos.get(dataNodeId).isAlive()) {
                /* get replica */
                try {
                  dataNodeInfos.get(dataNodeId).getDataNode().getBlock(blockId);
                } catch (RemoteException re) {
                  /* try next node */
                  continue;
                }
              }
            }
            /* try to place replica */
            while (true) {
              try {
                int dest = allocateBlock();
                dataNodeInfos.get(dest).getDataNode().putBlock(blockId, replica);
                commitBlockAllocation(dest, blockId);
                break;
              } catch (RemoteException re) {
                /* try next node */
                continue;
              }
            } /* while */
          } /* for */
        } /* catch */
      } /* for */
    } /* while */
  }

  static <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> getSortedEntries(Map<K, V> map) {
    SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
      new Comparator<Map.Entry<K,V>>() {
        @Override
        public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2)
        {
          int res = e1.getValue().compareTo(e2.getValue());
          return res != 0 ? res : 1;
        }
      }
    );
    sortedEntries.addAll(map.entrySet());
    return sortedEntries;
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
