package mr.core;

import java.io.File;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
	 * mapper_machine: pair of job, mapperID and machineID
	 * preset_mapper_ct: pair of mapper_id and count of mappers the scheduler allocated.
	 * preset_mapper_job_task: mapping of preset job_id and list of mapper_id
	 * finished_mapper_ct: pair of mapper_id and count of mappers that have finished.
	 * finished_mapper_job_task: mapping of finished job_id and list of mapper_id
	 * id_ssum: mapping of job_id, hashedID(Based on num of reducers) and the sum of size of blocks 
	 * 
	 * */
	Hashtable<String, Hashtable<String, String>> mapper_machine = new Hashtable<String, Hashtable<String, String>>();
	Hashtable<String, Integer> preset_mapper_ct = new Hashtable<String, Integer>();
	Hashtable<String, List<String>> preset_mapper_job_task = new Hashtable<String, List<String>>();
	
	Hashtable<String, Integer> finished_mapper_ct = new Hashtable<String, Integer>();
	Hashtable<String, List<String>> finished_mapper_job_task = new Hashtable<String, List<String>>();
	
	HashMap<String, HashMap<String, Integer>> id_ssum = new HashMap<String, HashMap<String, Integer>>();
	//JobID, MachineID, partitionID, size
	HashMap<String, HashMap<String, HashMap<String, Integer>>> job_mc_hash_size= new HashMap<String, HashMap<String, HashMap<String, Integer>>>();
	//Hashtable<String, List<String>> job_task_mapping = new Hashtable<String, List<String>>();
	HashMap<String, String> jobID_outputdir = new HashMap<String, String>();
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
	public HashMap<String, List<String>> preset_reducer(String jobID)
	{
		/* partition_res: mapping of machineID and hashedID */
		HashMap<String, List<String>> partition_res = new HashMap<String, List<String>>();
		HashMap<String, HashMap<String, Integer>> mc_hash_size = job_mc_hash_size.get(jobID);
		Iterator iter = mc_hash_size.entrySet().iterator(); 
		while (iter.hasNext()) { 
		    Map.Entry entry = (Map.Entry) iter.next(); 
		    String machineID = (String) entry.getKey();
		    HashMap<String, Integer> hash_size = (HashMap<String, Integer>)entry.getValue();
		    List<String> lst = new ArrayList<String>();
		    lst.add(machineID);
		    partition_res.put(machineID, lst);
		} 
		// Since I have to think more about how to optimize it, let's assume hashID = machineID
		System.out.println("In preset_reducer, partition_res:"+partition_res.toString());
		return partition_res;
		
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
			jobID_outputdir.put(job.get_jobId(), job.get_fileOutputPath());
			System.out.println("Scheduling Job "+job.get_jobId());
			JobStatus jstatus = new JobStatus();
			this.job_status.put(job.get_jobId(), jstatus);
			int ct = 0;
			Hashtable<String, String> mc_mp = new Hashtable<String, String>();
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
			   
			   mc_mp.put(mapper_id, String.valueOf(value.get(0)));
			   mapper_machine.put(job.get_jobId(), mc_mp);
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
	
	public Set get_mapper_machineIDs(String jobID)
	{
		Hashtable<String, String> mp_mc = mapper_machine.get(jobID);
		Iterator iter = mp_mc.entrySet().iterator(); 
		Set mcIDs = new HashSet();
		while (iter.hasNext())
		{
			Map.Entry entry = (Map.Entry) iter.next(); 
		    String key = (String) entry.getKey(); 
		    String val = (String) entry.getValue(); 
			mcIDs.add(val);
		}
		System.out.println("JobID:"+jobID+" has mcIDs:"+mcIDs.toString());
		return mcIDs;
		
	}
	
	public void shuffle(String jobID, HashMap<String, List<String>> mcID_hashIDs)
	{
		System.out.println("Shuffling ....");
		Iterator iter = mcID_hashIDs.entrySet().iterator(); 
		Set mcIDsets = get_mapper_machineIDs(jobID);
			
		//System.out.println("Machine IDS:"+mcIDs.toString());
		while (iter.hasNext()) { 
			Iterator mc_iter = mcIDsets.iterator();
		    Map.Entry entry = (Map.Entry) iter.next(); 
		    String machineID = (String) entry.getKey(); 		    
		    List<String> hashIDs = (List<String>) entry.getValue();
		    try {
				TaskTracker w_taskTracker = (TaskTracker) registry.lookup("TaskTracker_"+machineID);
				String w_path = "tmp/" + jobID + '/' + machineID + '/';
				
				while(mc_iter.hasNext())
				{				
					String curr = mc_iter.next().toString();
					
					if (curr.equals(machineID))
						continue;
					TaskTracker r_taskTracker = (TaskTracker) registry.lookup("TaskTracker_"+curr);
					String r_path = "tmp/" + jobID + '/' + curr + '/';
					for (int i = 0;i< hashIDs.size(); i++)
					{
						List<String> names = r_taskTracker.read_dir(r_path+'/', hashIDs.get(i));
						for (int j = 0; j < names.size(); j++)
						{
							String content = r_taskTracker.readstr(r_path+'/', names.get(j));
							w_taskTracker.writestr(w_path+'/'+names.get(j)+'_', content);
							System.out.println("Wrote to path:"+w_path+'/'+names.get(j)+'_');							
						}
					}
	
				}
		    } catch (AccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		
		//TaskTracker tt = (TaskTracker)registry.lookup("TaskTracker_"+String.valueOf(value.get(0)));
	}

	public void start_reducer(HashMap<String, List<String>> mcID_hashIDs)
	{
		Iterator iter = mcID_hashIDs.entrySet().iterator();
		while(iter.hasNext())
		{
			Map.Entry entry = (Map.Entry) iter.next();
			String mcID = (String)entry.getKey();
			List<String> hashIDs = (List<String>)entry.getValue();
			try {
				TaskTracker tt = (TaskTracker)registry.lookup("TaskTracker_"+mcID);
			
			} catch (AccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
		}
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
				/*
				 * ret: hashID->size
				 * */
				HashMap<String, Integer> ret = (HashMap<String, Integer>)msg.getContent();
				System.out.println("JobID:"+msg.getJob_id()+
								   ", TaskID:"+ msg.getTask_id()+
						           ", HashedID->Size:"+ret.toString());
			    
				String machineID = msg.getMachine_id();
			    if (finished_mapper_ct.containsKey(jobID))
				{
					int ct = finished_mapper_ct.get(jobID);
					ct += 1;
					finished_mapper_ct.put(jobID, ct);
				    List<String> mapper_ids = finished_mapper_job_task.get(jobID);
				    mapper_ids.add(taskID);
					finished_mapper_job_task.put(jobID, mapper_ids);
					HashMap<String, HashMap<String, Integer>> mc_hash_size = job_mc_hash_size.get(jobID);
					mc_hash_size.put(machineID, ret);
					job_mc_hash_size.put(jobID, mc_hash_size);
				}
				else
				{
					finished_mapper_ct.put(jobID, 1);
					List<String> mapper_ids = new ArrayList<String>();
					mapper_ids.add(taskID);
					finished_mapper_job_task.put(jobID, mapper_ids);
					HashMap<String, HashMap<String, Integer>> mc_hash_size = new HashMap<String, HashMap<String, Integer>>();
					mc_hash_size.put(machineID, ret);
					job_mc_hash_size.put(jobID, mc_hash_size);
				}
				if (finished_mapper_ct.get(jobID) == preset_mapper_ct.get(jobID))
				{
					/*
					 * Ensure locality, firstly preset reducers based on the HashID 
					 * generated by Context, which reports to TaskTracker, 
					 * and then JobTracker.
					 * 
					 * */
					HashMap<String, List<String>> mcID_hashIDs = preset_reducer(jobID);
					
					shuffle(jobID, mcID_hashIDs);
					
					start_reducer(mcID_hashIDs);
				}
				
			}
	}
}
