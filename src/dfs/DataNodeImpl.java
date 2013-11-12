package dfs;

import java.lang.Thread;
import java.lang.InterruptedException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Implementation for remote interface DataNode
 * @author Yanan Jian
 * @author Erdong Li
 */
public class DataNodeImpl implements DataNode
{
  private int id;
  private String dir;
  private String registryHost;
  private int registryPort;
  private List<Integer> blockIds;

  /**
   * Constructor
   * @param id datanode's id
   * @param dir directory for blocks
   * @param registryHost registry's host
   * @param registryPort registry's port number
   */
  public DataNodeImpl(int id, String dir, String registryHost, int registryPort)
  {
    this.id = id;
    this.dir = dir;
    this.registryHost = registryHost;
    this.registryPort = registryPort;
    blockIds = new LinkedList<Integer>();
  }

  /**
   * Get DataNode's ID
   * @return datanode's id
   * @throws RemoteException
   */
  @Override
  public int getId() throws RemoteException
    { return this.id; }

  /**
   * Put a block with blockId and given content to the DataNode
   * @param blockId block's id
   * @param content content in string format
   * @throws RemoteException
   */
  @Override
  public void putBlock(int blockId, String content) throws RemoteException
  {
    blockIds.add(blockId);
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(dir + blockId));
      bw.write(content);
      bw.close();
    } catch (IOException e) {
      throw new RemoteException("putBlock", e);
    }
  }

  /**
   * Put a block with blockId and given content to the DataNode
   * @param blockId block's id
   * @param content content in byte format
   * @throws RemoteException
   */
  @Override
  public void putBlock(int blockId, byte[] content) throws RemoteException
  {
    blockIds.add(blockId);
    try {
      FileOutputStream fileOutputStream = new FileOutputStream(new File(dir + blockId));
      fileOutputStream.write(content);
      fileOutputStream.close();
    } catch (IOException e) {
      throw new RemoteException("putBlock", e);
    }
  }

  /**
   * Retrieve a block with blockId and given content to the DataNode directly from another DataNode
   * @param datanode the sender
   * @param blockId block's id
   * @throws RemoteException
   */
  @Override
  public void putBlock(DataNode datanode, int blockId) throws RemoteException
  {
    blockIds.add(blockId);
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(dir + blockId));
      bw.write(datanode.getBlock(blockId));
      bw.close();
    } catch (IOException e) {
      throw new RemoteException("putBlock", e);
    }
  }

  /**
   * Retrieve content from block with given block ID
   * @param blockId block's id
   * @return block content in string format
   * @throws RemoteException
   */
  @Override
  public String getBlock(int blockId) throws RemoteException
  {
    try {
      BufferedReader br = new BufferedReader(new FileReader(dir + blockId));
      StringBuilder content = new StringBuilder();
      String line = null;
      while ((line = br.readLine()) != null) {
        content.append(line);
        content.append("\n");
      }
      br.close();
      return content.toString();
    } catch (Exception e) {
      throw new RemoteException("getBlock", e);
    }
  }

  /**
   * Retrieve content from block with given block ID in byte
   * @param blockId block's id
   * @return block content in string format
   * @throws RemoteException
   */
  @Override
  public byte[] getByteBlock(int blockId) throws RemoteException
  {
    try {
      File file = new File(dir + blockId);
      byte[] bFile = new byte[(int) file.length()];
      FileInputStream fileInputStream = new FileInputStream(file);
      fileInputStream.read(bFile);
      fileInputStream.close();
      return bFile;
    } catch (Exception e) {
      throw new RemoteException("getByteBlock", e);
    }
  }

  /**
   * Get directory for data blocks
   * @return directory for data blocks
   * @throws RemoteException
   */
  @Override
  public String getDir() throws RemoteException
    { return dir; }

  /**
   * Heat beat method for health check
   * @throws RemoteException;
   */
  @Override
  public void heartBeat() throws RemoteException
    {}

  /**
   * Terminate the DataNode
   * @throws RemoteException
   */
  @Override
  public void terminate() throws RemoteException
    { UnicastRemoteObject.unexportObject(this, true); }

  /**
   * Bootstrap phase, register itself to NameNode
   * @param port service's port number
   */
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
        System.out.println("NameNode unreachable, retry in 5 seconds");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Main method for DataNodeImpl
   * @param args command-line arguments
   */
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
