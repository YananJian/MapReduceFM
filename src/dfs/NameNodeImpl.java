package dfs;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
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

  public void register(int id, DataNode datanode) throws RemoteException
  {
    if (dataNodeInfos.get(id) != null) {
      dataNodeInfos.get(id).setDataNode(datanode);
      dataNodeInfos.get(id).setAlive(true);
    } else {
      dataNodeInfos.put(id, new DataNodeInfo(id, datanode));
    }
  }

  public int createFile(String filename, int nReplicas) throws RemoteException
  {
    if (fileInfos.get(filename) != null)
      /* file already exists */
      return 0;
    /* otherwise */
    if (nReplicas == 0)
      nReplicas = nReplicasDefault;
    fileInfos.put(filename, new FileInfo(filename, nReplicas));
    return nReplicas;
  }

  public int getBlockSize() throws RemoteException
    { return blockSize; }

  public int getNextBlockId(String filename) throws RemoteException
  {
    int blockId = 0;
    synchronized(blockCounter) {
      blockId =  blockCounter++;
    }
    fileInfos.get(filename).addBlockId(blockId);
    blockInfos.put(blockId, new BlockInfo(blockId, filename));
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

  public void commitBlockAllocation(int dataNodeId, String filename, int blockId) throws RemoteException
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

  public void terminate(String fsImageDir) throws RemoteException
  {
    try {
      /* write metadata to fsImage */
      File fsImage = new File(fsImageDir);
      BufferedWriter bw = new BufferedWriter(new FileWriter(fsImage));
      /* write file info */
      bw.write(fileInfos.size());
      bw.newLine();
      for (Map.Entry<String, FileInfo> entry : fileInfos.entrySet()) {
        FileInfo fi = (FileInfo) entry.getValue();
        bw.write(fi.toFsImage());
        bw.newLine();
      }
      /* write block info */
      bw.write(blockInfos.size());
      bw.newLine();
      for (Map.Entry<Integer, BlockInfo> entry : blockInfos.entrySet()) {
        BlockInfo bi = (BlockInfo) entry.getValue();
        bw.write(bi.toFsImage());
        bw.newLine();
      }
      /* write datanode info */
      bw.write(dataNodeInfos.size());
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
        dni.getDataNode().terminate();
      }

      /* terminate namenode */
      System.exit(1);
    } catch (Exception e) {
      throw new RemoteException("Exception caught", e);
    }
  }

  public void bootstrap(int nDataNodes, String fsImageDir)
  {
    /* register itself to registry */
    try {
      NameNode stub = (NameNode) UnicastRemoteObject.exportObject(this, 0);
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
          for (int j = 2; j < words.length; j++)
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
            /* get filename */
            String filename = blockInfos.get(blockId).getFileName();
            /* get all datanodes that has this block */
            List<Integer> dataNodeIds = blockInfos.get(blockId).getDataNodeIds();
            String replica = null;
            /* assume datanodes that have this replica won't all fail at the same time */
            for (int dataNodeId : dataNodeIds) {
              if (dataNodeInfos.get(dataNodeId).isAlive()) {
                /* get replica */
                try {
                  replica = dataNodeInfos.get(dataNodeId).getDataNode().getBlock(blockId);
                  break;
                } catch (RemoteException re) {
                  /* try next node */
                }
              }
            }
            /* try to place replica */
            while (true) {
              try {
                int dest = allocateBlock();
                dataNodeInfos.get(dest).getDataNode().putBlock(blockId, replica);
                commitBlockAllocation(dest, filename, blockId);
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
    String fsImageDir = args[5];
    NameNodeImpl namenode = new NameNodeImpl(nReplicasDefault, healthCheckInterval, blockSize, port);
    /* bootstrap */
    namenode.bootstrap(nDataNodes, fsImageDir);
    /* register to registry and start health check */
    namenode.healthCheck();
  }
}
