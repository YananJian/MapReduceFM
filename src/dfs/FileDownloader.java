package dfs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Facility to download a file from DFS
 * @author Yanan Jian
 * @author Erdong Li
 */
public class FileDownloader
{
  private String path;
  private String filename;
  private String registryHost;
  private int registryPort;

  /**
   * Constructor
   * @param path local path to store the file
   * @param filename filename on dfs
   * @param registryHost registry's host
   * @param registryPort registry's port number
   */
  public FileDownloader(String path, String filename, String registryHost, int registryPort)
  {
    this.path = path;
    this.filename = filename;
    this.registryHost = registryHost;
    this.registryPort = registryPort;
  }

  /**
   * Download a file and write to local path using RMI calls
   * @throws IOException
   * @throws NotBoundException
   * @throws RemoteException
   */
  public void download() throws IOException, NotBoundException,  RemoteException
  {
    /* get all blocks and its datanodes */
    Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
    NameNode namenode = (NameNode) registry.lookup("NameNode");
    Map<Integer, List<Integer>> blocks = namenode.getAllBlocks(filename);
    BufferedWriter bw = new BufferedWriter(new FileWriter(path));
    for (Map.Entry<Integer, List<Integer>> entry : blocks.entrySet()) {
      int blockId = entry.getKey();
      List<Integer> dataNodeIds = entry.getValue();
      boolean success = false;
      /* try to get block from datanodes */
      for (Integer dataNodeId : dataNodeIds) {
        DataNode datanode = namenode.getDataNode(dataNodeId);
        try {
          bw.write(datanode.getBlock(blockId));
          success = true;
          break;
        } catch (RemoteException e) {
          /* datanode is dead */
          continue;
        }
      }
      if (!success) {
        bw.close();
        throw new RemoteException("download failed: all datanode for block #" + blockId + " is dead");
      }
    }
    bw.close();
  }

  /**
   * Main method of FileDownloader
   * @param args command-line arguments
   */
  public static void main(String[] args)
  {
    FileDownloader downloader = null;
    try {
      String path = args[0];
      String filename = args[1];
      String registryHost = args[2];
      int registryPort = Integer.parseInt(args[3]);
      downloader = new FileDownloader(path, filename, registryHost, registryPort);
    } catch (Exception e) {
      System.out.println("usage: <path> <filename> <registry host>");
      System.exit(1);
    }
    try {
      downloader.download();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
