package dfs;

import java.lang.Thread;
import java.lang.InterruptedException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.net.Socket;
import java.net.ServerSocket;

public class DataNode
{
  private int id;
  private int port;
  private String nameNodeHost;
  private int nameNodePort;
  private String dir;
  private LinkedList<Integer> blockIds;

  public DataNode(int id, int port, String nameNodeHost, int nameNodePort, String dir)
  {
    this.id = id;
    this.port = port;
    this.nameNodeHost = nameNodeHost;
    this.nameNodePort = nameNodePort;
    this.dir = dir;
    blockIds = new LinkedList<Integer>();
  }

  public int getId()
    { return id; }

  public String getNameNodeHost()
    { return nameNodeHost; }

  public int getNameNodePort()
    { return nameNodePort; }

  public void putBlock(int blockId, String content)
  {
    blockIds.add(blockId);
    File block = new File(dir + blockId);
    try {
      block.createNewFile();
      BufferedWriter bw = new BufferedWriter(new FileWriter(block.getAbsoluteFile()));
      bw.write(content);
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void run()
  {
    /* collect all blocks under dir */
    File folder = new File(dir);
    File[] files = folder.listFiles();
    for (File file : files)
      blockIds.add(Integer.parseInt(file.getName()));

    /* register to name node */
    while (true) {
      try {
        Socket socket = new Socket(nameNodeHost, nameNodePort);
        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
        oos.writeObject(new RegisterMessage(id, blockIds));
        oos.close();
        socket.close();
        break;
      } catch (Exception e) {
        /* NameNode not reader, keep trying */
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ie) {
          e.printStackTrace();
          System.exit(1);
        }
        continue;
      }
    }

    /* start service */
    ServerSocket server = null;
    try {
      server = new ServerSocket(port);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
    while (true) {
      try {
        new DataNodeHandler(server.accept(), this).run();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args)
  {
    DataNode dn = new DataNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2], Integer.parseInt(args[3]), args[4]);
    dn.run();
  }
}
