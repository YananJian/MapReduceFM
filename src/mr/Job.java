package mr;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.UUID;

import mr.common.Constants.JOB_STATUS;
import mr.core.*;
import dfs.NameNode;
import mr.common.Constants.*;

public class Job implements java.io.Serializable{
	
	private static final long serialVersionUID = 1L;
	Class<? extends Mapper<?, ?, ?, ?>> mapper = null;
	Class<? extends Reducer<?, ?, ?, ?>> reducer = null;
	String fileInputPath = null;
	String fileOutputPath = null;
	String fname = null;
	private Registry registry = null;
	NameNode namenode = null;
	JobTracker jobTracker = null;
	String job_id = null;
	int mapper_ct = 0;
	int reducer_ct = 0;
	HashMap<String, TASK_STATUS> mapper_status = new HashMap<String, TASK_STATUS>();
	HashMap<String, TASK_STATUS> reducer_status = new HashMap<String, TASK_STATUS>();
	JOB_STATUS stat = null;
	
	public Job(String host, int port)
	{
		try {
			registry = LocateRegistry.getRegistry(host, port);
			jobTracker = (JobTracker) registry.lookup("JobTracker");
			Long uuid = Math.abs(UUID.randomUUID().getMostSignificantBits());
			job_id = String.valueOf(uuid);
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (NotBoundException e) {
			e.printStackTrace();
		}
		
	}
	
	public void set_jobStatus(JOB_STATUS stat)
	{
		this.stat = stat;
	}
	
	public JOB_STATUS get_jobStatus()
	{
		return this.stat;
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
	
	public void set_mapper(Class<? extends Mapper<?, ?, ?, ?>> class1)
	{
		this.mapper = class1;
	}
	
	public Class<? extends Mapper<?, ?, ?, ?>> get_mapper()
	{
		return this.mapper;
	}
	
	public void set_reducer(Class<? extends Reducer<?, ?, ?, ?>> class1)
	{
		this.reducer = class1;
	}
	
	public Class<? extends Reducer<?, ?, ?, ?>> get_reducer()
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
	
	
	public void submit() throws RemoteException
	{
		jobTracker.schedule(this);
	}
}
