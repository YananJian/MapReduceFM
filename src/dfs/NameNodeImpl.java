package dfs;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.lang.Thread;
import java.util.Comparator;
import java.util.List;
import java.util.LinkedList;
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

/**
 * Implementation of remote interface NameNode
 * @author Yanan Jian
 * @author Erdong Li
 */
public class NameNodeImpl implements NameNode
{
  private boolean terminated;
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
   * @param nReplicasDefault default replication factor
   * @param healthCheckInterval health check interval in seconds
   * @param blockSize partition size
   * @param registryPort registry's port number
   */
  public NameNodeImpl(int nReplicasDefault, int healthCheckInterval, int blockSize, int registryPort)
  {
    this.terminated = false;
    this.nReplicasDefault = nReplicasDefault;
    this.healthCheckInterval = healthCheckInterval;
    this.blockSize = blockSize;
    blockCounter = 0;
    try {
      registry = LocateRegistry.createRegistry(registryPort);
    } catch (RemoteException e) {
      e.printStackTrace();
      System.exit(1);
    }
    fileInfos = new Hashtable<String, FileInfo>();
    blockInfos = new Hashtable<Integer, BlockInfo>();
    dataNodeInfos = new TreeMap<Integer, DataNodeInfo>();
  }

  /**
   * Register a DataNode to this NameNode
   * @param id datanode;s id
   * @param datanode stub of datanode
   * @throws RemoteException
   */
  @Override
  public void register(int id, DataNode datanode) throws RemoteException
  {
    if (terminated)
      throw new RemoteException("DFS terminating");
    if (dataNodeInfos.get(id) != null)
      dataNodeInfos.get(id).setDataNode(datanode);
    else
      dataNodeInfos.put(id, new DataNodeInfo(id, datanode));
    dataNodeInfos.get(id).setAlive(true);
    System.out.println("DataNode " + id + " registered");
  }

  /**
   * Retrieve a stub of DataNode with given ID
   * @return stub of datanode
   * @throws RemoteException
   */
  @Override
  public DataNode getDataNode(int id) throws RemoteException
  {
    if (terminated)
      throw new RemoteException("DFS terminating");
    return dataNodeInfos.get(id).getDataNode();
  }

  /**
   * Create metadata for given filename
   * @param filename filename
   * @param nReplicas requested replication factor
   * @return actual replication factor
   * @throws RemoteException
   */
  @Override
  public int createFile(String filename, int nReplicas) throws RemoteException
  {
    if (terminated)
      throw new RemoteException("DFS terminating");
    if (fileInfos.get(filename) != null)
      /* file already exists */
      return 0;
    /* otherwise */
    if (nReplicas == 0 || nReplicas > dataNodeInfos.size())
      nReplicas = nReplicasDefault;
    fileInfos.put(filename, new FileInfo(filename, nReplicas));
    return nReplicas;
  }

  /**
   * Retrieve the default block size
   * @return default block size
   * @throws RemoteException
   */
  @Override
  public int getBlockSize() throws RemoteException
  {
    if (terminated)
      throw new RemoteException("DFS terminating");
    return blockSize;
  }

  /**
   * Retrieve block ID for the next block
   * @param filename filename for the block
   * @return block id for the next block
   * @throws RemoteException
   */
  @Override
  public int getNextBlockId(String filename) throws RemoteException
  {
    if (terminated)
      throw new RemoteException("DFS terminating");
    int blockId = 0;
    synchronized(blockCounter) {
      blockId =  blockCounter++;
    }
    fileInfos.get(filename).addBlockId(blockId);
    blockInfos.put(blockId, new BlockInfo(blockId, filename));
    return blockId;
  }

  /**
   * Retrieve the next DataNode for storing the replica
   * @return the next datanode chosen
   * @throws RemoteException
   */
  @Override
  public DataNode allocateBlock() throws RemoteException
  {
    if (terminated)
      throw new RemoteException("DFS terminating");
    /* select DataNode with least #blocks */
    SortedSet<Map.Entry<Integer, DataNodeInfo>> sortedEntries = getSortedEntries(dataNodeInfos);
    for (Map.Entry<Integer, DataNodeInfo> entry : sortedEntries)
      if (entry.getValue().isAlive())
        return entry.getValue().getDataNode();
    return null;
  }

  /**
   * Commit the block allocation
   * @param dataNodeId datanode id that sotres the block
   * @param filename filename of the block
   * @param blockId block id
   * @throws RemoteException
   */
  @Override
  public void commitBlockAllocation(int dataNodeId, String filename, int blockId) throws RemoteException
  {
    if (terminated)
      throw new RemoteException("DFS terminating");
    blockInfos.get(blockId).addDataNode(dataNodeId);
    dataNodeInfos.get(dataNodeId).addBlock(blockId);
  }

  /**
   * Retrieve all block IDs with their DataNode IDs for the file with given filename
   * @param filename filename of the blocks
   * @return a map from block id to datanode ids
   * @throws RemoteException
   */
  @Override
  public Map<Integer, List<Integer>> getAllBlocks(String filename) throws RemoteException
  {
    if (terminated)
      throw new RemoteException("DFS terminating");
    FileInfo fi = fileInfos.get(filename);
    Map<Integer, List<Integer>> result = new LinkedHashMap<Integer, List<Integer>>();
    List<Integer> blockIds = fi.getBlockIds();
    for (int blockId : blockIds) {
      List<Integer> aliveIds = new LinkedList<Integer>();
      List<Integer> dataNodeIds = blockInfos.get(blockId).getDataNodeIds();
      for (int id : dataNodeIds)
        if (dataNodeInfos.get(id).isAlive())
          aliveIds.add(id);
      result.put(blockId, aliveIds);
    }
    return result;
  }

  /**
   * Describe information of the DFS
   * @return human-readable string representing the dfs
   * @throws RemoteException
   */
  @Override
  public String describeDFS() throws RemoteException
  {
    if (terminated)
      throw new RemoteException("DFS terminating");
    StringBuilder dfs = new StringBuilder();
    dfs.append("========= dfs info =========\n");
    dfs.append("#default replicas: " + nReplicasDefault + "\n");
    dfs.append("healthcheck interval: " + healthCheckInterval + " second(s)\n");
    dfs.append("block size: " + blockSize + " byte\n");
    dfs.append("\n========= file info =========\n");
    for (Map.Entry<String, FileInfo> entry : fileInfos.entrySet())
      dfs.append(entry.getValue().toString() + "\n");
    dfs.append("\n========= block info =========\n");
    for (Map.Entry<Integer, BlockInfo> entry : blockInfos.entrySet())
      dfs.append(entry.getValue().toString() + "\n");
    dfs.append("\n========= datanode info =========\n");
    for (Map.Entry<Integer, DataNodeInfo> entry : dataNodeInfos.entrySet())
      dfs.append(entry.getValue().toString() + "\n");
    return dfs.toString();
  }

  /**
   * Terminate all nodes, and write metadata to fsImage
   * @param fsImageDir directory to fsImage
   * @throws RemoteException
   */
  @Override
  public void terminate(String fsImageDir) throws RemoteException
  {
    try {
      terminated = true;
      /* write metadata to fsImage */
      File fsImage = new File(fsImageDir);
      BufferedWriter bw = new BufferedWriter(new FileWriter(fsImage));
      /* write file info */
      bw.write(String.valueOf(fileInfos.size()));
      bw.newLine();
      for (Map.Entry<String, FileInfo> entry : fileInfos.entrySet()) {
        FileInfo fi = (FileInfo) entry.getValue();
        bw.write(fi.toFsImage());
        bw.newLine();
      }
      /* write block info */
      bw.write(String.valueOf(blockInfos.size()));
      bw.newLine();
      for (Map.Entry<Integer, BlockInfo> entry : blockInfos.entrySet()) {
        BlockInfo bi = (BlockInfo) entry.getValue();
        bw.write(bi.toFsImage());
        bw.newLine();
      }
      /* write datanode info */
      bw.write(String.valueOf(dataNodeInfos.size()));
      bw.newLine();
      for (Map.Entry<Integer, DataNodeInfo> entry : dataNodeInfos.entrySet()) {
        DataNodeInfo dni = (DataNodeInfo) entry.getValue();
        bw.write(dni.toFsImage());
        bw.newLine();
      }
      bw.close();

      /* terminate datanode */
      for (Map.Entry<Integer, DataNodeInfo> entry : dataNodeInfos.entrySet()) {
        DataNodeInfo dni = (DataNodeInfo) entry.getValue();
        if (dni.isAlive())
          dni.getDataNode().terminate();
      }

      /* terminate namenode */
      registry.unbind("NameNode");
      UnicastRemoteObject.unexportObject(this, true);
      UnicastRemoteObject.unexportObject(registry, true);
    } catch (Exception e) {
      throw new RemoteException("Exception caught", e);
    }
  }

  /**
   * Bootstrap phase, read metadata from fsImage, and wait for DataNodes to bootstrap
   * @param nDataNodes number of datanodes
   * @param fsImageDir directory of fsImage
   * @param port service's port
   */
  public void bootstrap(int nDataNodes, String fsImageDir, int port)
  {
    /* register itself to registry */
    try {
      NameNode stub = (NameNode) UnicastRemoteObject.exportObject(this, port);
      registry.rebind("NameNode", stub);
    } catch (RemoteException e) {
      e.printStackTrace();
      System.exit(1);
    }

    /* init metadata from fsImage */
    File fsImage = new File(fsImageDir);
    if (fsImage.exists()) {
      try {
        BufferedReader br = new BufferedReader(new FileReader(fsImage));
        /* parse fileInfos */
        int n = Integer.parseInt(br.readLine());
        for (int i = 0; i < n; i++) {
          String line = br.readLine();
          String[] words = line.split(" ");
          FileInfo fi = new FileInfo(words[0], Integer.parseInt(words[1]));
          for (int j = 2; j < words.length; j++)
            fi.addBlockId(Integer.parseInt(words[j]));
          fileInfos.put(words[0], fi);
        }
        /* parse blockInfos */
        n = Integer.parseInt(br.readLine());
        for (int i = 0; i < n; i++) {
          String line = br.readLine();
          String[] words = line.split(" ");
          BlockInfo bi = new BlockInfo(Integer.parseInt(words[0]), words[1]);
          for (int j = 2; j < words.length; j++)
            bi.addDataNode(Integer.parseInt(words[j]));
          blockInfos.put(Integer.parseInt(words[0]), bi);
        }
        /* parse dataNodeInfos */
        n = Integer.parseInt(br.readLine());
        for (int i = 0; i < n; i++) {
          String line = br.readLine();
          String[] words = line.split(" ");
          DataNodeInfo dni = new DataNodeInfo(Integer.parseInt(words[0]), null);
          for (int j = 1; j < words.length; j++)
            dni.addBlock(Integer.parseInt(words[j]));
          dataNodeInfos.put(Integer.parseInt(words[0]), dni);
        }
        br.close();
      } catch (Exception e) {
        System.out.println("fsImage corrupted");
        System.exit(1);
      }
    }

    /* wait for each datanode to get online */
    int currDataNodes = 0;
    while (currDataNodes < nDataNodes) {
      currDataNodes = 0;
      for (Map.Entry<Integer, DataNodeInfo> entry : dataNodeInfos.entrySet())
        if (entry.getValue().isAlive())
          currDataNodes++;
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    System.out.println("Bootstrap finishes");
  }

  /**
   * Check DataNode's HeartBeat periodically, and create new replicas upon DataNode failure
   */
  public void healthCheck()
  {
    /* start healthcheck */
    while (!terminated) {
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
          System.out.println("Lose connection with DataNode " + dni.getId());
          /* unreachable node */
          dni.setAlive(false);
          /* move each replica to other node */
          List<Integer> blockIds = dni.getBlockIds();
          for (int blockId : blockIds) {
            /* get filename */
            String filename = blockInfos.get(blockId).getFileName();
            /* get all datanodes that has this block */
            List<Integer> dataNodeIds = blockInfos.get(blockId).getDataNodeIds();
            /* assume datanodes that have this replica won't all fail at the same time */
            while (!terminated) {
              DataNode datanode = null;
              try {
                datanode = allocateBlock();
              } catch (RemoteException re) {
                continue;
              }
              boolean success = false;
              for (int dataNodeId : dataNodeIds) {
                if (dataNodeInfos.get(dataNodeId).isAlive()) {
                  try {
                    /* try to place replica */
                    datanode.putBlock(dataNodeInfos.get(dataNodeId).getDataNode(), blockId);
                    success = true;
                    break;
                  } catch (RemoteException re) {
                    /* try next node */
                  }
                }
              }
              if (success) {
                try {
                  commitBlockAllocation(datanode.getId(), filename, blockId);
                } catch (RemoteException re) {
                  continue;
                }
                break;
              }
            }
          } /* for */
        } /* catch */
      } /* for */
    } /* while */
  }

  /**
   * Static method that sort map entries
   * @param map map to be sorted
   * @return sorted set that contains map entries
   */
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


  /**
   * Main method for NameNodeImpl
   * @param args command-line arguments
   */
  public static void main(String[] args)
  {
    int nReplicasDefault = Integer.parseInt(args[0]);
    int healthCheckInterval = Integer.parseInt(args[1]);
    int blockSize = Integer.parseInt(args[2]);
    int registryPort = Integer.parseInt(args[3]);
    int nDataNodes = Integer.parseInt(args[4]);
    String fsImageDir = args[5];
    int port = Integer.parseInt(args[6]);
    NameNodeImpl namenode = new NameNodeImpl(nReplicasDefault, healthCheckInterval, blockSize, registryPort);
    /* bootstrap */
    namenode.bootstrap(nDataNodes, fsImageDir, port);
    /* register to registry and start health check */
    namenode.healthCheck();
  }
}
