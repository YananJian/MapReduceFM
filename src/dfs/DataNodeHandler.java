package dfs;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.Runnable;
import java.net.Socket;

public class DataNodeHandler implements Runnable
{
  private Socket socket;
  private DataNode dataNode;

  public DataNodeHandler(Socket socket, DataNode dataNode)
  {
    this.socket = socket;
    this.dataNode = dataNode;
  }

  public void run()
  {
    try {
      ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
      Message msg = (Message) ois.readObject();
      if (msg instanceof HeartBeat) {
        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
        oos.writeObject(new HeartBeatACK(dataNode.getId()));
        oos.close();
      } else if (msg instanceof PutBlockRequest) {
        PutBlockRequest request = (PutBlockRequest) msg;
        dataNode.putBlock(request.getBlockId(), request.getContent());
      }
      ois.close();
      socket.close();
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }
}
