package dfs;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.lang.Thread;
import java.util.Hashtable;
import java.net.ServerSocket;
import java.net.Socket;

public class NameNode
{
  private int port;
  private int replication;
  private int counter;
  private Integer blockCounter;
  private Hashtable<String, FileInfo> fileInfos;
  private Hashtable<Integer, BlockInfo> blockInfos;
  private Hashtable<Integer, DataNodeInfo> dataNodeInfos;

  /**
   * Constructor
   * @param port port of NameNode
   */
  public NameNode(int port, int replication)
  {
    this.port = port;
    this.replication = replication;
    blockCounter = 0;
    counter = 0;
    fileInfos = new Hashtable<String, FileInfo>();
    blockInfos = new Hashtable<Integer, BlockInfo>();
    dataNodeInfos = new Hashtable<Integer, DataNodeInfo>();
  }

  public void createFile(String filename, int nReplicas)
  {
    if (nReplicas == 0)
      nReplicas = replication;
    fileInfos.put(filename, new FileInfo(filename, nReplicas));
  }

  public BlockInfo allocateBlock(String filename)
  {
    FileInfo fi = fileInfos.get(filename);
    int nReplicas = fi.getNReplicas();
    int blockId = 0;
    synchronized(blockCounter) {
      blockId = blockCounter++;
    }
    BlockInfo bi = new BlockInfo(blockId);
    fi.addBlockId(blockId);
    /* select DataNode with round-robin policy */
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

  public void addDataNode(int dataNodeId, String host, int port)
  {
    DataNodeInfo dni = new DataNodeInfo(dataNodeId, host, port);
    dataNodeInfos.put(dataNodeId, dni);
  }

  public DataNodeInfo getDataNodeInfo(int dataNodeId)
    { return dataNodeInfos.get(dataNodeId); }

  public void run()
  {
    ServerSocket server = null;
    try {
      server = new ServerSocket(port);
    } catch (IOException e) {
      /* cannot do anything */
      e.printStackTrace();
      System.exit(1);
    }

    /* bootstrap phase, wait for each datanode to get online */
    for (int i = 0; i < dataNodeInfos.size();) {
      try {
        Socket socket = server.accept();
        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        RegisterMessage msg = (RegisterMessage) ois.readObject();
        dataNodeInfos.get(msg.getDataNodeId()).setAlive(true);
        ois.close();
        socket.close();
        i++;
      } catch (Exception e) {
        /* cannot do anything */
        e.printStackTrace();
        continue;
      }
    }

    /* start service */
    new NameNodeServer(server, this).run();

    /* start healthcheck */
    while (true) {
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      for (int i = 0; i < dataNodeInfos.size(); i++) {
        DataNodeInfo dni = dataNodeInfos.get(i);
        if (dni.isAlive()) {
          dni.setAlive(false);
          try {
            Socket socket = new Socket(dni.getHost(), dni.getPort());
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(new HeartBeat());
            oos.close();
            socket.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  public static void main(String[] args)
  {
    NameNode nn = new NameNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
    int nDataNodes = Integer.parseInt(args[2]);
    for (int i = 0; i < nDataNodes; i++)
      nn.addDataNode(i, args[3+2*i], Integer.parseInt(args[4+2*i]));
    /* start execution */
    nn.run();
  }
}
