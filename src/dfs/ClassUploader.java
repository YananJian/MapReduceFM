package dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Facility to upload a .class file to DFS
 * @author Yanan Jian
 * @author Erdong Li
 */
public class ClassUploader
{
  private String path;
  private String filename;
  private int nReplicas;
  private String registryHost;
  private int registryPort;

  /**
   * Constructor
   * @param path local path to store the file
   * @param filename filename on dfs
   * @param nReplicas replication factor
   * @param registryHost registry's host
   * @param registryPort registry's port number
   */
  public ClassUploader(String path, String filename, int nReplicas, String registryHost, int registryPort) throws IOException
  {
    this.path = path;
    this.filename = filename;
    this.nReplicas = nReplicas;
    this.registryHost = registryHost;
    this.registryPort = registryPort;
  }

  /**
   * Upload a .class file from local path to dfs using RMI calls
   * @throws IOException
   * @throws NotBoundException
   * @throws RemoteException
   */
  public void upload() throws IOException, RemoteException, NotBoundException
  {
    File file = new File(path);
    FileInputStream fileInputStream = new FileInputStream(file);
    byte[] bFile = new byte[(int) file.length()];
    fileInputStream.read(bFile);
    fileInputStream.close();
    /* create metadata for namenode */
    Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
    NameNode namenode = (NameNode) registry.lookup("NameNode");
    if ((nReplicas = namenode.createFile(filename, nReplicas)) == 0)
      throw new IOException("File already exists");
    /* upload content to datanodes */
    int blockId = namenode.getNextBlockId(filename);
    for (int i = 0; i < nReplicas; i++) {
      while (true) {
        DataNode datanode = namenode.allocateBlock();
        try {
          datanode.putBlock(blockId, bFile);
        } catch (RemoteException e) {
          /* datanode dead */
          e.printStackTrace();
          continue;
        }
        namenode.commitBlockAllocation(datanode.getId(), filename, blockId);
        break;
      }
    }
  }

  /**
   * Main method of ClassUploader
   * @param args command-line arguments
   */
  public static void main(String[] args)
  {
    ClassUploader uploader = null;
    try {
      String path = args[0];
      String filename = args[1];
      int nReplicas = Integer.parseInt(args[2]);
      String registryHost = args[3];
      int registryPort = Integer.parseInt(args[4]);
      uploader = new ClassUploader(path, filename, nReplicas, registryHost, registryPort);
    } catch (Exception e) {
      System.out.println("usage: <path> <filename> <#replicas>");
      System.exit(1);
    }
    try {
      uploader.upload();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
