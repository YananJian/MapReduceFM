package dfs;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.Runnable;
import java.net.Socket;

public class NameNodeServerHandler implements Runnable
{
  private Socket socket;
  private NameNode nameNode;

  public NameNodeServerHandler(Socket socket, NameNode nameNode)
  {
    this.socket = socket;
    this.nameNode = nameNode;
  }

  public void run()
  {
    try {
      ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
      Message msg = (Message) ois.readObject();
      if (msg instanceof CreateFileRequest) {
        CreateFileRequest request = (CreateFileRequest) msg;
        nameNode.createFile(request.getFileName(), request.getNReplicas());
      }
      else if (msg instanceof AllocateBlockRequest) {
        AllocateBlockRequest request = (AllocateBlockRequest) msg;
        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
        //oos.writeObject(new AllocateBlockResponse(nameNode.allocateBlock(request.getFileName())));
        oos.close();
      }
      else if (msg instanceof GetDataNodeInfoRequest) {
        GetDataNodeInfoRequest request = (GetDataNodeInfoRequest) msg;
        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
        //oos.writeObject(new GetDataNodeInfoResponse(nameNode.getDataNodeInfo(request.getDataNodeId())));
        oos.close();
      }
      socket.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
