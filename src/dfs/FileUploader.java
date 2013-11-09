package dfs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class FileUploader
{
  private String path;
  private String filename;
  private int nReplicas;
  private String registryHost;
  private int registryPort;

  public FileUploader(String path, String filename, int nReplicas, String registryHost, int registryPort)
  {
    this.path = path;
    this.filename = filename;
    this.nReplicas = nReplicas;
    this.registryHost = registryHost;
    this.registryPort = registryPort;
  }

  public void upload() throws IOException, RemoteException, NotBoundException
  {
    /* create metadata for namenode */
    Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
    NameNode namenode = (NameNode) registry.lookup("NameNode");
    if ((nReplicas = namenode.createFile(filename, nReplicas)) == 0)
      throw new IOException("File already exists");
    /* upload content to datanodes */
    int blockSize = namenode.getBlockSize();
    int blockId = namenode.getNextBlockId(filename);
    BufferedReader br = new BufferedReader(new FileReader(path));
    StringBuilder content = new StringBuilder();
    String line = "";
    while (line != null) {
      line = br.readLine();
      if (line == null || (content.length() != 0 && content.length() + line.length() > blockSize)) {
        /* buffer full, put block to datanodes */
        for (int i = 0; i < nReplicas; i++) {
          while (true) {
            int dataNodeId = namenode.allocateBlock();
            DataNode datanode = (DataNode) registry.lookup(Integer.toString(dataNodeId));
            try {
              datanode.putBlock(blockId, content.toString());
            } catch (RemoteException e) {
              /* datanode dead */
              e.printStackTrace();
              continue;
            }
            namenode.commitBlockAllocation(dataNodeId, filename, blockId);
            break;
          }
        }
        /* clear the buffer */
        content.setLength(0);
        /* get new blockId */
        if (line != null)
          blockId = namenode.getNextBlockId(filename);
      }
      content.append(line);
      content.append("\n");
    }
    br.close();
  }

  public static void main(String[] args)
  {
    String path = args[0];
    String filename = args[1];
    int nReplicas = Integer.parseInt(args[2]);
    String registryHost = args[3];
    int registryPort = Integer.parseInt(args[4]);
    FileUploader uploader = new FileUploader(path, filename, nReplicas, registryHost, registryPort);
    try {
      uploader.upload();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
