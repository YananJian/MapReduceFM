package dfs;

import java.io.IOException;
import java.lang.Runnable;
import java.net.ServerSocket;

public class NameNodeServer implements Runnable
{
  private ServerSocket server;
  private NameNode nameNode;

  public NameNodeServer(ServerSocket server, NameNode nameNode)
  {
    this.server = server;
    this.nameNode = nameNode;
  }

  public void run()
  {
    while (true)
      try {
        new NameNodeServerHandler(server.accept(), nameNode).run();
      } catch (IOException e) {
        e.printStackTrace();
      }
  }
}
