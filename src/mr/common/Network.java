package mr.common;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import mr.common.Msg;

public class Network {

	public static Msg communicate(String ip, int port, Msg msg)
	{
		Socket sock;
    	Msg ret_msg = null;
		try {
			sock = new Socket(ip, port);
			ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
	    	ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
	    	
	    	oos.writeObject(msg);
	    	ret_msg = (Msg)ois.readObject();
	    	
	    	sock.close();
	    	return ret_msg;
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			System.out.println("Can not connect to Registry Server");
		}catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
			System.out.println("Registry server is not returning Msg");
    	}	
		return ret_msg;
	}
}
