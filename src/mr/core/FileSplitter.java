package mr.core;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import conf.Config;
import mr.common.*;
import mr.common.Constants.MSG_TP;

public class FileSplitter {
	
	ArrayList<String> blk_ids = new ArrayList<String>();
	
	private String request_block() //request block from NameNode, get block id
	{
		Msg msg = new Msg();
		msg.setMsg_tp(MSG_TP.REQ_BLK);
		Msg ret_msg = Network.communicate(Config.MASTER_IP, Config.MASTER_PORT, msg);
		String blk_id = (String)ret_msg.getContent();
		return blk_id;
	}
	
	private void splitSingleFile(String fpath)
	{
		try {
			String blk_id = request_block();
			FileInputStream fs = new FileInputStream(fpath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs));
			String line = null;
			String buffer[] = new String[Config.SPLIT_SIZE];
			int cnt = 0;
			while ((line = br.readLine()) != null)
			{
				if (cnt >= Config.SPLIT_SIZE)
				{
					//write line to block
					System.out.println("Writing buffer to block "+blk_id);
					blk_ids.add(blk_id);
					cnt = 0;
					buffer = null;
					blk_id = request_block();
				}
				if (buffer == null)
				{
					buffer = new String[Config.SPLIT_SIZE];			
				}
				buffer[cnt] = line;
				cnt ++;			
			}
			// in case we have incomplete buffer
			if (buffer != null)
				//write line to block
			{
				System.out.println("Writing buffer to block "+blk_id);
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void readFiles(String fpath)
	{
		File f = new File(fpath);
		if (f.isDirectory())
		{
			String[] fnames = f.list();
			for (int i = 0; i< fnames.length; i++)
				splitSingleFile(fpath + '/'+fnames[i]);
		}
		else
			splitSingleFile(fpath);
	}
	
	public static void main(String args[])
	{
		FileSplitter fsp = new FileSplitter();
		fsp.readFiles("/Users/yanan/javapro/workspace/testfiles");
	}
}
