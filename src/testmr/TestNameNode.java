package testmr;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;

import mr.Mapper;
import mr.common.*;
import mr.common.Constants.MSG_TP;
import mr.core.Scheduler;
import conf.Config;

public class TestNameNode {

	static Hashtable<String, String> free_blks = new Hashtable<String, String>();
	static Hashtable<String, String> preallocated_blks = new Hashtable<String, String>();
	
	public void init()
	{
		free_blks.put("computeNode1", Config.COMPUTE_A_IP);
	}
	
	public static Hashtable<String, String> get_free_blks()
	{
		return free_blks;
	}
	
	public static Hashtable<String, String> get_preallocated_blks()
	{
		return preallocated_blks;
	}
	
	public void run()
	{
		try {
			ServerSocket listener = new ServerSocket(Config.MASTER_PORT);
			while (true)
			{
				Socket sock = listener.accept();
				try {
					Msg ret_msg = new Msg();
                    ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
                    ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                	
                	Msg msg = (Msg) ois.readObject();              	
                	System.out.println(" > connection from host:" + sock.getInetAddress()+":"+sock.getPort());
                	if (msg.getMsg_tp() == MSG_TP.START_MAPPER)                		
                	{
                		Mapper mapper = (Mapper)msg.getContent();
                		Scheduler.assign_map(mapper);
                		ret_msg.setContent("Start Mapper");
                	}
                	else if(msg.getMsg_tp() == MSG_TP.TERMINATE_MAPPER)
                		ret_msg.setContent("Terminate Mapper");
                	else if (msg.getMsg_tp() == MSG_TP.REQ_BLK)                		
                		{
                			ret_msg.setContent("computeNode_1");              			
                			free_blks.remove("computeNode_1");
                			preallocated_blks.put("computeNode_1", Config.COMPUTE_A_IP);               		
                		}
                	oos.writeObject(ret_msg);
                                
                } catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (EOFException e)
				{
					
				}
				finally {
                    sock.close();
                }
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String args[])
	{
		TestNameNode tnn = new TestNameNode();
		tnn.run();
	}
}
