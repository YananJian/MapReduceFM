package testmr;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import mr.Mapper;
import mr.common.Msg;
import mr.common.Constants.MSG_TP;
import conf.Config;

public class TestComputeNode {
	
	
	
	public void start_map(Mapper mapper, String fpath)
	{	
		try {
			FileInputStream fs = new FileInputStream(fpath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs));
			String line = null;
			while ((line = br.readLine())!=null)
				mapper.map(line, "");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run()
	{
		try {
			ServerSocket listener = new ServerSocket(Config.METADATATBL_PORT);
			while (true)
			{
				Socket sock = listener.accept();
				try {
					Msg ret_msg = new Msg();
                    ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
                    ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
                	
                	Msg msg = (Msg) ois.readObject();              	
                	System.out.println(" > connection from host:" + sock.getInetAddress()+":"+sock.getPort());
                	if (msg.getMsg_tp() == MSG_TP.REQ_BLK)                		
                		ret_msg.setContent("computeNode_1");
                	else if (msg.getMsg_tp() == MSG_TP.START_MAPPER)
                	{
                		Mapper mapper = (Mapper)msg.getContent();
                		String fpath = msg.getBlk_fpath();
                		start_map(mapper, fpath);
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
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
