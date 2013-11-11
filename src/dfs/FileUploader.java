package dfs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Facility to upload a file to DFS
 * @author Yanan Jian
 * @author Erdong Li
 */
public class FileUploader
{
  private BufferedReader br;
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
  public FileUploader(String path, String filename, int nReplicas, String registryHost, int registryPort) throws IOException
  {
    this.br = new BufferedReader(new FileReader(path));
    this.filename = filename;
    this.nReplicas = nReplicas;
    this.registryHost = registryHost;
    this.registryPort = registryPort;
  }

  /**
   * Constructor
   * @param br BufferedReader for input
   * @param filename filename on dfs
   * @param nReplicas replication factor
   * @param registryHost registry's host
   * @param registryPort registry's port number
   */
  public FileUploader(BufferedReader br, String filename, int nReplicas, String registryHost, int registryPort)
  {
    this.br = br;
    this.filename = filename;
    this.nReplicas = nReplicas;
    this.registryHost = registryHost;
    this.registryPort = registryPort;
  }

  /**
   * Upload a file from local path to dfs using RMI calls
   * @throws IOException
   * @throws NotBoundException
   * @throws RemoteException
   */
  public void upload() throws IOException, RemoteException, NotBoundException
  {
    /* create metadata for namenode */
    Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
    NameNode namenode = (NameNode) registry.lookup("NameNode");
    if ((nReplicas = namenode.createFile(filename, nReplicas)) == 0)
      throw new IOException("File already exists");
    /* upload content to datanodes */
    int blockSize = namenode.getBlockSize();
    StringBuilder content = new StringBuilder();
    String line = "";
    while (line != null) {
      line = br.readLine();
      if (line == null || (content.length() != 0 && content.length() + line.length() > blockSize)) {
        /* buffer full, put block to datanodes */
        int blockId = namenode.getNextBlockId(filename);
        for (int i = 0; i < nReplicas; i++) {
          while (true) {
            DataNode datanode = namenode.allocateBlock();
            try {
              datanode.putBlock(blockId, content.toString());
            } catch (RemoteException e) {
              /* datanode dead */
              e.printStackTrace();
              continue;
            }
            namenode.commitBlockAllocation(datanode.getId(), filename, blockId);
            break;
          }
        }
        /* clear the buffer */
        content.setLength(0);
      }
      content.append(line);
      content.append("\n");
    }
    br.close();
  }

  /**
   * Main method of FileUploader
   * @param args command-line arguments
   */
  public static void main(String[] args)
  {
    FileUploader uploader = null;
    try {
      String path = args[0];
      String filename = args[1];
      int nReplicas = Integer.parseInt(args[2]);
      String registryHost = args[3];
      int registryPort = Integer.parseInt(args[4]);
      uploader = new FileUploader(path, filename, nReplicas, registryHost, registryPort);
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
