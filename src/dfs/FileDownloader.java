package dfs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class FileDownloader
{
  private String path;
  private String filename;
  private String registryHost;
  private int registryPort;

  public FileDownloader(String path, String filename, String registryHost, int registryPort)
  {
    this.path = path;
    this.filename = filename;
    this.registryHost = registryHost;
    this.registryPort = registryPort;
  }

  public void download() throws IOException, NotBoundException,  RemoteException
  {
    /* get all blocks and its datanodes */
    Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
    NameNode namenode = (NameNode) registry.lookup("NameNode");
    Map<Integer, List<Integer>> blocks = namenode.getAllBlocks(filename);
    BufferedWriter bw = new BufferedWriter(new FileWriter(path));
    Iterator<Map.Entry<Integer, List<Integer>>> itor = blocks.entrySet().iterator();
    while (itor.hasNext()) {
      Map.Entry<Integer, List<Integer>> entry = itor.next();
      int blockId = entry.getKey();
      List<Integer> dataNodeIds = entry.getValue();
      boolean success = false;
      for (Integer dataNodeId : dataNodeIds) {
        DataNode datanode = (DataNode) registry.lookup(dataNodeId.toString());
        try {
          bw.write(datanode.getBlock(blockId));
          success = true;
          break;
        } catch (RemoteException e) {
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

  public static void main(String[] args)
  {
    String path = args[0];
    String filename = args[1];
    String registryHost = args[2];
    int registryPort = Integer.parseInt(args[3]);
    FileDownloader downloader = new FileDownloader(path, filename, registryHost, registryPort);
    try {
      downloader.download();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
