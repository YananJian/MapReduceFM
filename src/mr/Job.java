package mr;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.UUID;

import mr.common.Constants.JOB_STATUS;
import mr.common.Constants.MSG_TP;
import mr.common.Msg;
import mr.common.Network;
import mr.core.*;
import conf.Config;
import dfs.FileUploader;
import dfs.NameNode;


public class Job implements java.io.Serializable{
	
	
	private static final long serialVersionUID = 1L;
	Class<? extends Mapper> mapper = null;
	Class<? extends Reducer> reducer = null;
	String fileInputPath = null;
	String fileOutputPath = null;
	String fname = null;
	private Registry registry = null;
	private String registryHost;
	private int registryPort;
	NameNode namenode = null;
	JobTracker jobTracker = null;
	String job_id = null;
	
	public Job()
	{
		try {
			registry = LocateRegistry.getRegistry(Config.MASTER_IP, Config.MASTER_PORT);
			jobTracker = (JobTracker) registry.lookup("JobTracker");
			Long uuid = Math.abs(UUID.randomUUID().getMostSignificantBits());
			job_id = String.valueOf(uuid);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public String get_jobId()
	{
		return this.job_id;
	}
	
	public void set_mapper(Class<? extends Mapper> class1)
	{
		this.mapper = class1;
	}
	
	public Class<? extends Mapper> get_mapper()
	{
		return this.mapper;
	}
	
	public void set_reducer(Class<? extends Reducer> class1)
	{
		this.reducer = class1;
	}
	
	public void set_fileInputPath(String path)
	{
		this.fileInputPath = path;
		String tmp[] = path.split("/");
		fname = tmp[tmp.length-1];
	}
	
	public void set_fileOutputPath(String path)
	{
		this.fileOutputPath = path;
	}
	
	public String get_fileOutputPath()
	{
		return this.fileOutputPath;
	}
	
	public String get_fileName()
	{
		return this.fname;
	}
	
	private void registerJob()
	{
		double d = System.currentTimeMillis();  
        d += System.nanoTime() ;         
		
		String job_id = String.valueOf((int)d);
		Msg msg = new Msg();
		msg.setJob_id(job_id);
		msg.setMsg_tp(MSG_TP.SET_JOB_STATUS);
		msg.setJob_stat(JOB_STATUS.STARTING);
		Msg ret_msg = Network.communicate(Config.MASTER_IP, Config.MASTER_PORT, msg);
		System.out.println(ret_msg.getContent());
	}
	
	public void submit() throws RemoteException
	{
		// Register job in MetaDataTBL, set job status to Starting
		//registerJob();
		int num_replicas = 3;
		// FileUploader begin to split and upload file (require block, read till meet split size, write to block)
		FileUploader fu = new FileUploader(this.fileInputPath, 
				                           fname, 
				                           num_replicas, 
				                           Config.MASTER_IP, 
				                           Config.MASTER_PORT);	
		// Scheduler assign task to Compute Nodes
		try {
			fu.upload();
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Set job status to Started in MetaDataTBL
		
		jobTracker.schedule(this);
	}

	

}
