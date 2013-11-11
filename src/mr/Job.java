package mr;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.UUID;

import mr.common.Constants.JOB_STATUS;
import mr.common.Constants.MSG_TP;
import mr.common.Msg;
import mr.common.Network;
import mr.core.*;
import conf.Config;
import dfs.FileUploader;
import dfs.NameNode;
import mr.common.Constants.*;

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
	int mapper_ct = 0;
	int reducer_ct = 0;
	HashMap<String, TASK_STATUS> mapper_status = new HashMap<String, TASK_STATUS>();
	HashMap<String, TASK_STATUS> reducer_status = new HashMap<String, TASK_STATUS>();
	
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
	
	public void set_mapperStatus(String mapper_id, TASK_STATUS status)
	{
		mapper_status.put(mapper_id, status);
	}
	
	public void set_reducerStatus(String reducer_id, TASK_STATUS status)
	{
		reducer_status.put(reducer_id, status);
	}
	
	public HashMap<String, TASK_STATUS> get_mapperStatus()
	{
		return mapper_status;
	}
	
	public HashMap<String, TASK_STATUS> get_reducerStatus()
	{
		return reducer_status;
	}
	
	public void inc_mapperct()
	{
		this.mapper_ct += 1;
	}
	
	public void inc_reducerct()
	{
		this.reducer_ct += 1;
	}
	
	public int get_mapperct()
	{
		return this.mapper_ct;
	}
	
	public int get_reducerct()
	{
		return this.reducer_ct;
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
	
	public Class<? extends Reducer> get_reducer()
	{
		return this.reducer;
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
		try {
		FileUploader fu = new FileUploader(this.fileInputPath, 
				                           fname, 
				                           num_replicas, 
				                           Config.MASTER_IP, 
				                           Config.MASTER_PORT);	
		// Scheduler assign task to Compute Nodes
		
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
