package dfs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  public void download() throws Exception
  {
    /* get all blocks and its datanodes */
    Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
    NameNode namenode = (NameNode) registry.lookup("NameNode");
    Map<Integer, List<Integer>> blocks = namenode.getAllBlocks(filename);
    BufferedWriter bw = new BufferedWriter(new FileWriter(path));
    Iterator<Map.Entry<Integer, List<Integer>>> itor = blocks.entrySet().iterator();
    while (itor.hasNext()) {
      Map.Entry<Integer, List<Integer>> entry = itor.next();
    }
  }
}
