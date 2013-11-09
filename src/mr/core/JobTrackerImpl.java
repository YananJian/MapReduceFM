package mr.core;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mr.Job;
import mr.JobStatus;
import mr.common.Constants.*;
import mr.common.Msg;
import conf.Config;
import dfs.NameNode;


public class JobTrackerImpl implements JobTracker{
	String registryHost = Config.MASTER_IP;
	int registryPort = Config.MASTER_PORT;
	NameNode namenode = null;
	Hashtable<String, JobStatus> job_status = new Hashtable<String, JobStatus>();
	/*
	 * preset_mapper_ct: pair of mapper_id and count of mappers the scheduler allocated.
	 * preset_mapper_job_task: mapping of preset job_id and list of mapper_id
	 * finished_mapper_ct: pair of mapper_id and count of mappers that have finished.
	 * finished_mapper_job_task: mapping of finished job_id and list of mapper_id
	 * id_ssum: mapping of job_id, hashedID(Based on num of reducers) and the sum of size of blocks 
	 * partition_res: mapping of mapper_id and hashedID
	 * */
	Hashtable<String, Integer> preset_mapper_ct = new Hashtable<String, Integer>();
	Hashtable<String, List<String>> preset_mapper_job_task = new Hashtable<String, List<String>>();
	
	Hashtable<String, Integer> finished_mapper_ct = new Hashtable<String, Integer>();
	Hashtable<String, List<String>> finished_mapper_job_task = new Hashtable<String, List<String>>();
	
	HashMap<String, HashMap<String, Integer>> id_ssum = new HashMap<String, HashMap<String, Integer>>();
	HashMap<String, String> partition_res = new HashMap<String, String>();
	//Hashtable<String, List<String>> job_task_mapping = new Hashtable<String, List<String>>();
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
	
	/*
	 * preset_reducer:
	 * Sum the sizes of partitions given back by mapper tasks,
	 * Choose the Top N (N = num of reducers) sizes, for each partition,
	 * allocate reducers on the machine that has most resources.
	 * 
	 * Eg. We have 2 reducers.
	 * Machine A has partition k1 -> 1KB, k2 -> 1MB
	 * Machine B has partition k1 -> 1MB, k2 -> 1GB
	 * Machine C has partition k1 -> 1GB, k2 -> 1TB
	 * sum(k1) = 1GB + 1MB + 1KB
	 * sum(k2) = 1TB + 1GB + 1MB
	 * 
	 * If schedule reducer on A, A has to pull 1MB+1GB, 1GB+1TB files
	 * If schedule reducer on B, B has to pull 1KB+1GB, 1MB+1TB files
	 * If schedule reducer on C, C has to pull 1KB+1MB, 1MB+1GB files
	 * 
	 * C has the most resources for k1 and k2, 
	 * Thus we choose to schedule 2 reducers on C.
	 * */
	public void preset_reducer(String jobID, String mapperID, HashMap<String, Integer> id_size)
	{
		/*
		Iterator iter = id_size.entrySet().iterator(); 
		while (iter.hasNext()) { 
		    Map.Entry entry = (Map.Entry) iter.next(); 
		    String key = (String) entry.getKey(); 
		    Integer val = (Integer)entry.getValue();
		   
		} */
		// Since I have to think more about how to optimize it, let's assume hashID = machineID
		
		//partition_res.put(mapperID, value)
		
	}

	
	public void update_job_status(String job_id, String task_id, TASK_TP tp, JOB_STATUS status)
	{
		JobStatus jstatus = this.job_status.get(job_id);
		if (tp == TASK_TP.MAPPER)
			jstatus.set_mapper_status(task_id, status);
		else if (tp == TASK_TP.REDUCER)
			jstatus.set_reducer_status(task_id, status);
		this.job_status.put(job_id, jstatus);
	}
	
	private void update_preset_mapper(String jobID, String taskID, int ct)
	{	
		preset_mapper_ct.put(jobID, ct);
		List<String> mapper_ids = preset_mapper_job_task.get(jobID);
		if (mapper_ids == null)
		{
			List<String> tmp = new ArrayList<String>();
			preset_mapper_job_task.put(jobID, tmp);
		}
		else
		{
			mapper_ids.add(taskID);
			preset_mapper_job_task.put(jobID, mapper_ids);
		}				
	}
	
	public void schedule(Job job)
	{
		try {
			Map<Integer, List<Integer>> mappings = this.namenode.getAllBlocks(job.get_fileName());
			Set<?> set = mappings.entrySet();
			System.out.println("Scheduling Job "+job.get_jobId());
			JobStatus jstatus = new JobStatus();
			this.job_status.put(job.get_jobId(), jstatus);
			int ct = 0;
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
			   _jstatus.set_mapper_status(mapper_id, JOB_STATUS.STARTING);
			   this.job_status.put(job.get_jobId(), _jstatus); 	
			   
			   TaskTracker tt = (TaskTracker)registry.lookup("TaskTracker_"+String.valueOf(value.get(0)));
			  
			   tt.set_reducer_ct(Config.REDUCER_NUM);
			   /* 
			    * TaskTracker has to hash key to an ID based on REDUCER_NUM 
			    * Thus to ensure same key on different mappers will have the same ID
			    * So finally same key will go to the same reducer.
			    * After Context iterated over all the keys, (Thus after the first stage of partition)
			    * push the {key:id} hashmap to TaskTracker, TaskTracker then pass it to JobTracker.
			    * JobTracker then coordinate which ID goes to which reducer (locality considered).
			    * 
			    * */
			   tt.start_map(job.get_jobId(), mapper_id, String.valueOf(key), job.get_mapper());
			   ct += 1;
			   update_preset_mapper(job.get_jobId(), mapper_id, ct);
			   System.out.println("Job Tracker trying to start map task on "+String.valueOf(value.get(0)));
			   System.out.println("MapperID:"+mapper_id);
			  }
			preset_mapper_ct.put(job.get_jobId(), ct);
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
	
	public void shuffle(String jobID)
	{
		System.out.println("Shuffling ....");
		
	}

	@Override
	public void heartbeat(Msg msg) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("Got Msg!, TaskType:"+msg.getTask_tp().toString());
		
		if (msg.getTask_tp() == TASK_TP.MAPPER)
			if (msg.getTask_stat() == TASK_STATUS.FINISHED)
			{
				String jobID = msg.getJob_id();
				String taskID = msg.getTask_id();
				System.out.println("Mapper Finished!");	
				HashMap<String, Integer> ret = (HashMap<String, Integer>)msg.getContent();
				System.out.println("JobID:"+msg.getJob_id()+
								   ", TaskID:"+ msg.getTask_id()+
						           ", Return val:"+ret.toString());
				if (finished_mapper_ct.containsKey(jobID))
				{
					int ct = finished_mapper_ct.get(jobID);
					ct += 1;
					finished_mapper_ct.put(jobID, ct);
				    List<String> mapper_ids = finished_mapper_job_task.get(jobID);
				    mapper_ids.add(taskID);
					finished_mapper_job_task.put(jobID, mapper_ids);
				}
				else
				{
					finished_mapper_ct.put(jobID, 1);
					List<String> mapper_ids = new ArrayList<String>();
					mapper_ids.add(taskID);
					finished_mapper_job_task.put(jobID, mapper_ids);
				}
				if (finished_mapper_ct.get(jobID) == preset_mapper_ct.get(jobID))
				{
					/*
					 * Ensure locality, firstly preset reducers based on the HashID 
					 * generated by Context, which reports to TaskTracker, 
					 * and then JobTracker.
					 * 
					 * */
					preset_reducer(jobID, taskID, ret);
					shuffle(jobID);
				}
				
			}
	}
}
