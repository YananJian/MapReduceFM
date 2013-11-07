package mr.core;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mr.Job;
import mr.JobStatus;
import mr.common.Constants;
import conf.Config;
import dfs.NameNode;

public class JobTrackerImpl implements JobTracker{
	String registryHost = Config.MASTER_IP;
	int registryPort = Config.MASTER_PORT;
	NameNode namenode = null;
	Hashtable<String, JobStatus> job_status = new Hashtable<String, JobStatus>();
	
	Hashtable<String, List<String>> job_task_mapping = new Hashtable<String, List<String>>();
	Registry registry = null;
	
	public JobTrackerImpl()
	{
		
	}
	
	public void init()
	{
		try {
	        registry = LocateRegistry.getRegistry(registryHost, registryPort);
	        
	        JobTracker stub = (JobTracker) UnicastRemoteObject.exportObject(this, 0);
	        registry.rebind("JobTracker", stub);
	        this.namenode = (NameNode) registry.lookup("NameNode");
	        
	        System.out.println("Registered");
	       
	      } catch (Exception e) {
	        
	        e.printStackTrace();
	        
	      }
	}
	
	public void update_job_status(String job_id, String task_id, Constants.TASK_TP tp, Constants.JOB_STATUS status)
	{
		JobStatus jstatus = this.job_status.get(job_id);
		if (tp == Constants.TASK_TP.MAPPER)
			jstatus.set_mapper_status(task_id, status);
		else if (tp == Constants.TASK_TP.REDUCER)
			jstatus.set_reducer_status(task_id, status);
		this.job_status.put(job_id, jstatus);
	}
	
	public void schedule(Job job)
	{
		try {
			Map<Integer, List<Integer>> mappings = this.namenode.getAllBlocks(job.get_fileName());
			Set<?> set = mappings.entrySet();
			System.out.println("Scheduling Job "+job.get_jobId());
			JobStatus jstatus = new JobStatus();
			this.job_status.put(job.get_jobId(), jstatus);
			for(Iterator<?> iter = set.iterator(); iter.hasNext();)
			  {
			   @SuppressWarnings("rawtypes")
			   Map.Entry entry = (Map.Entry)iter.next();			   
			   Integer key = (Integer)entry.getKey();
			   @SuppressWarnings("unchecked")
			   List<Integer> value = (List<Integer>)entry.getValue();
			   System.out.println(String.valueOf(key) +" :" + String.valueOf(value.get(0)));
			   
			   /* set mapper task */
			   JobStatus _jstatus = this.job_status.get(job.get_jobId());
			   String mapper_id = job.get_jobId() + "_" + String.valueOf(key);
			   _jstatus.set_mapper_status(mapper_id, Constants.JOB_STATUS.STARTING);
			   this.job_status.put(job.get_jobId(), _jstatus); 	
			   
			   TaskTracker tt = (TaskTracker)registry.lookup("TaskTracker_"+String.valueOf(value.get(0)));
			   tt.start_map(job.get_jobId(), mapper_id, String.valueOf(key), job.get_mapper());
			   System.out.println("Job Tracker trying to start map task on "+String.valueOf(value.get(0)));
			   System.out.println("MapperID:"+mapper_id);
			  }
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JobTrackerImpl jt = new JobTrackerImpl();
		jt.init();
	}
}
