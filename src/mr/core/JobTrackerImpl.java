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
import java.util.Map.Entry;
import java.util.Set;

import mr.Job;
import mr.JobStatus;
import mr.Reducer;
import mr.common.Constants.*;
import mr.common.Msg;
import conf.Config;
import dfs.NameNode;


public class JobTrackerImpl implements JobTracker{
	String registryHost = Config.MASTER_IP;
	int registryPort = Config.MASTER_PORT;
	NameNode namenode = null;
	int reducer_ct = Config.REDUCER_NUM;
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
	Hashtable<String, Hashtable<String, String>> reducer_machine = new Hashtable<String, Hashtable<String, String>> ();
	Hashtable<String, Integer> preset_mapper_ct = new Hashtable<String, Integer>();
	Hashtable<String, List<String>> preset_mapper_job_task = new Hashtable<String, List<String>>();
	
	Hashtable<String, Integer> finished_mapper_ct = new Hashtable<String, Integer>();
	Hashtable<String, List<String>> finished_mapper_job_task = new Hashtable<String, List<String>>();
	
	HashMap<String, HashMap<String, Integer>> id_ssum = new HashMap<String, HashMap<String, Integer>>();
	//JobID, MachineID, partitionID, size
	HashMap<String, HashMap<String, HashMap<String, Integer>>> job_mc_hash_size= new HashMap<String, HashMap<String, HashMap<String, Integer>>>();
	//Hashtable<String, List<String>> job_task_mapping = new Hashtable<String, List<String>>();
	HashMap<String, String> jobID_outputdir = new HashMap<String, String>();
	HashMap<String, Job> jobID_Job = new HashMap<String, Job>();
	
	HashMap<String, Integer> cpu_resource = new HashMap<String, Integer>();
	Registry registry = null;
	
	
	public void init(String port)
	{
		try {
	        registry = LocateRegistry.getRegistry(registryHost, registryPort);
	        JobTracker stub = (JobTracker) UnicastRemoteObject.exportObject(this, Integer.valueOf(port));
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
	
	/*private void update_preset_mapper(String jobID, String taskID, int ct)
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
	}*/
	
	public void allocate_mapper(String machineID, String mapper_id, String blockID, Job job, Hashtable<String, String> mc_mp)
	{
		TaskTracker tt;
		try {
			tt = (TaskTracker)registry.lookup("TaskTracker_"+machineID);
			tt.set_reducer_ct(reducer_ct);
			mc_mp.put(mapper_id, machineID);
			mapper_machine.put(job.get_jobId(), mc_mp);
			System.out.println("prepare to start mapper");
			tt.start_map(job.get_jobId(), mapper_id, blockID, job.get_mapper());
			
			job.set_mapperStatus(mapper_id, TASK_STATUS.RUNNING);
			job.inc_mapperct();
			jobID_Job.put(job.get_jobId(), job);
			//update_preset_mapper(job.get_jobId(), mapper_id, ct);
			System.out.println("Job Tracker trying to start map task on "+String.valueOf(machineID)
					   												     +", mapperID:"+mapper_id);
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
	
	public String pick_idle_machine()
	{
		Iterator iter = this.cpu_resource.entrySet().iterator();
		String arbi = "";
		while(iter.hasNext())
		{
			Entry entry = (Entry) iter.next();
			String machineID = (String)entry.getKey();
			Integer res = (Integer) entry.getValue();
			if (res > 0)
				return machineID;
			arbi = machineID;
		}
		return arbi;
	}
	public void schedule(Job job)
	{
		try {
			Map<Integer, List<Integer>> mappings = this.namenode.getAllBlocks(job.get_fileName());
			jobID_Job.put(job.get_jobId(), job);
			Set<?> set = mappings.entrySet();
			if (job.get_fileOutputPath() != null)
				jobID_outputdir.put(job.get_jobId(), job.get_fileOutputPath());
			System.out.println("Scheduling Job "+job.get_jobId());
			
			
			Hashtable<String, String> mc_mp = new Hashtable<String, String>();
			for(Iterator<?> iter = set.iterator(); iter.hasNext();)
			  {
			   @SuppressWarnings("rawtypes")
			   Map.Entry entry = (Map.Entry)iter.next();			   
			   Integer blockID = (Integer)entry.getKey();
			   @SuppressWarnings("unchecked")
			   List<Integer> value = (List<Integer>)entry.getValue();
			   System.out.println("BlockID:"+blockID.toString());
			   /* set mapper task */
			  
			   String mapper_id = job.get_jobId() + "_m_" + String.valueOf(blockID);
			   
			   boolean allocated = false;
			   for (Integer machineID: value)
			   {
				   System.out.println("MachineID:"+machineID.toString());
				   int aval_cpus = this.cpu_resource.get(String.valueOf(machineID));
				   System.out.println("Aval CPU NUM:"+aval_cpus);
				   if (aval_cpus > 0)
				   {
					   System.out.println("Availble CPU, machine: "+machineID);
					   allocate_mapper(String.valueOf(machineID), mapper_id, String.valueOf(blockID), job, mc_mp);
					   aval_cpus --;
					   this.cpu_resource.put(String.valueOf(machineID), aval_cpus);
					   allocated = true;
					   break;
				   }				   
			   }
			   if (!allocated)
			   {
				   String machineID = pick_idle_machine();
				   int aval_cpus = this.cpu_resource.get(machineID);
				   allocate_mapper(machineID, mapper_id, String.valueOf(blockID), job, mc_mp);
				   aval_cpus --;
				   this.cpu_resource.put(machineID, aval_cpus);
				   allocated = true;
			   }
			  
			   /* 
			    * TaskTracker has to hash key to an ID based on REDUCER_NUM 
			    * Thus to ensure same key on different mappers will have the same ID
			    * So finally same key will go to the same reducer.
			    * After Context iterated over all the keys, (Thus after the first stage of partition)
			    * push the {key:id} hashmap to TaskTracker, TaskTracker then pass it to JobTracker.
			    * JobTracker then coordinate which ID goes to which reducer (locality considered).
			    * 
			    * */
			   			   
			  }
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
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

	public void start_reducer(String job_id, String write_path, HashMap<String, List<String>> mcID_hashIDs)
	{
		Iterator iter = mcID_hashIDs.entrySet().iterator();
		while(iter.hasNext())
		{
			Map.Entry entry = (Map.Entry) iter.next();
			String mcID = (String)entry.getKey();
			List<String> hashIDs = (List<String>)entry.getValue();
			try {
				TaskTracker tt = (TaskTracker)registry.lookup("TaskTracker_"+mcID);
				String reducer_id = job_id + "_r_" + String.valueOf(mcID);
				Job job = this.jobID_Job.get(job_id);
				Class<? extends Reducer> reducer = job.get_reducer();
				tt.start_reducer(job_id, reducer_id, write_path, reducer);
				Hashtable<String, String> rcmc = new Hashtable<String, String>();
				rcmc.put(reducer_id, mcID);
				reducer_machine.put(job_id, rcmc);
				System.out.println("Starting Reducer in JobTracker, job_id:"+job_id+", reducer id:"+reducer_id);
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
	
	public boolean is_task_finished(String jobID, TASK_TP tp)
	{
		Job job = jobID_Job.get(jobID);
		HashMap<String,TASK_STATUS> task_status = null;
		if (tp == TASK_TP.MAPPER)
			task_status = job.get_mapperStatus();
		else if (tp == TASK_TP.REDUCER)
			task_status = job.get_reducerStatus();
		Iterator iter = task_status.entrySet().iterator();
		int ct = 0;
		while(iter.hasNext())
		{
			Entry pairs = (Entry) iter.next();
			String mapperID = (String)pairs.getKey();
			TASK_STATUS status = (TASK_STATUS)pairs.getValue();
			if (status == TASK_STATUS.RUNNING)
			{
				System.out.println("Task "+mapperID+" is running");
				return false;
			}
			else if (status == TASK_STATUS.FINISHED)
				System.out.println("Task "+mapperID+" is finished");
		}
		return true;
	}
	
	@Override
	public void heartbeat(Msg msg) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("Got Msg!");
				
		if (msg.getTask_tp() == TASK_TP.MAPPER)
		{
			if (msg.getTask_stat() == TASK_STATUS.FINISHED)
			
			{
				String jobID = msg.getJob_id();
				String taskID = msg.getTask_id();
				String machineID = msg.getMachine_id();
				System.out.println("Mapper Finished!");	
				Job job = jobID_Job.get(jobID);
				job.set_mapperStatus(taskID, TASK_STATUS.FINISHED);
				System.out.println("Job Finished, writing to Hash, taskID:"+taskID);
				
				HashMap<String, Integer> ret = (HashMap<String, Integer>)msg.getContent();
				HashMap<String, HashMap<String, Integer>> mc_hash_size = new HashMap<String, HashMap<String, Integer>>();
				mc_hash_size.put(machineID, ret);
				job_mc_hash_size.put(jobID, mc_hash_size);
				if (is_task_finished(jobID, TASK_TP.MAPPER))
				{
					HashMap<String, List<String>> mcID_hashIDs = preset_reducer(jobID);
					shuffle(jobID, mcID_hashIDs);
					start_reducer(jobID, this.jobID_outputdir.get(jobID), mcID_hashIDs);
				}
				
			}
		}
		else if (msg.getTask_tp() == TASK_TP.REDUCER)
		{
			if (msg.getTask_stat() == TASK_STATUS.FINISHED)
			{
				String jobID = msg.getJob_id();
				String taskID = msg.getTask_id();
				System.out.println("Reducer Finished!");
				String machineID = msg.getMachine_id();
				Job job = jobID_Job.get(jobID);
				job.set_reducerStatus(taskID, TASK_STATUS.FINISHED);
				if (is_task_finished(jobID, TASK_TP.REDUCER))
				{
					System.out.println("All Reducer finished");
					JobStatus jstatus = this.job_status.get(jobID);
					jstatus.set_job_stat(JOB_STATUS.FINISHED);
					
				}
			}
		}
		
		if (msg.getMsg_tp() == MSG_TP.HEARTBEAT)
		{
			cpu_resource.put(msg.getMachine_id(), msg.get_aval_procs());
			System.out.println("Putting cpu num into cpu_resources, machineID:"+msg.getMachine_id()
					+", cpu num:"+msg.get_aval_procs());
		}
	}
	
    @Override
	public String desc_job(String jobID) throws RemoteException
	{
        /* retrieve necessary data */
        Job job = jobID_Job.get(jobID);
        int nMapper = job.get_mapperct();
        int nReducer = job.get_reducerct();
        Map<String, TASK_STATUS> mapperStatus = job.get_mapperStatus();
        Map<String, TASK_STATUS> reducerStatus = job.get_reducerStatus();
        /* construct status msg */
        StringBuilder sb = new StringBuilder();
        sb.append("Job ID: " + jobID + "\n");
        sb.append("Input File: " + job.get_fileName() + "\n");
        sb.append("Output Path: " + job.get_fileOutputPath() + "\n");
        sb.append("Mapper: " + job.get_mapper().getName() + "\t");
        sb.append("#Mapper Instance: " + nMapper + "\n");
        sb.append("Reducer: " + job.get_reducer().getName() + "\n");
        sb.append("#Reducer Instance: " + nReducer + "\n");
        int nFinishedMapper = 0;
        for (Map.Entry<String, TASK_STATUS> entry : mapperStatus.entrySet())
            if (entry.getValue() == TASK_STATUS.FINISHED)
                nFinishedMapper++;
        float mapperProgress = nFinishedMapper/nMapper*100;
        int nFinishedReducer = 0;
        for (Map.Entry<String, TASK_STATUS> entry : reducerStatus.entrySet())
            if (entry.getValue() == TASK_STATUS.FINISHED)
                nFinishedReducer++;
        float reducerProgress = nFinishedReducer/nReducer*100;
        sb.append("Progress: Mapper " + (int)mapperProgress + "%\tReducer: " + (int)reducerProgress + "\n");
		return sb.toString();
	}
	
    @Override
	public String desc_jobs() throws RemoteException
	{

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Job> entry : jobID_Job.entrySet()) {
            sb.append("--------------------------------------------------\n");
            sb.append(desc_job(entry.getKey()));
        }
        sb.append("--------------------------------------------------\n");
		return sb.toString();

	}

    private void terminate_mappers(String jobID, HashMap<String,TASK_STATUS> mapper_status)
    {
    	Iterator miter = mapper_status.entrySet().iterator();
		while(miter.hasNext())
		{
			Entry pairs = (Entry) miter.next();
			String mapperID = (String)pairs.getKey();
			String machineID = mapper_machine.get(jobID).get(mapperID);
			
;			TASK_STATUS status = (TASK_STATUS)pairs.getValue();
			if (status == TASK_STATUS.RUNNING)
			{
				TaskTracker tt;
				try {
					tt = (TaskTracker) registry.lookup("TaskTracker_"+machineID);
					tt.terminate(mapperID);
					System.out.println("Task "+mapperID +" is terminated");
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
			else if (status == TASK_STATUS.FINISHED)
				System.out.println("Task "+mapperID+" is finished");
		}
    	
    }
    private void terminate_reducers(String jobID, HashMap<String,TASK_STATUS> reducer_status)
    {
    	Iterator riter = reducer_status.entrySet().iterator();
		while(riter.hasNext())
		{
			Entry pairs = (Entry) riter.next();
			String reducerID = (String)pairs.getKey();
			String machineID = reducer_machine.get(jobID).get(reducerID);
			
;			TASK_STATUS status = (TASK_STATUS)pairs.getValue();
			if (status == TASK_STATUS.RUNNING)
			{
				TaskTracker tt;
				try {
					tt = (TaskTracker) registry.lookup("TaskTracker_"+machineID);
					tt.terminate(reducerID);
					System.out.println("Task "+reducerID +" is terminated");
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
			else if (status == TASK_STATUS.FINISHED)
				System.out.println("Task "+reducerID+" is finished");
		}
    }
    
	public void terminate_job(String jobID)
	{
		Job job = jobID_Job.get(jobID);
		HashMap<String,TASK_STATUS> mapper_status = null;
		HashMap<String,TASK_STATUS> reducer_status = null;
		
		mapper_status = job.get_mapperStatus();
		
		reducer_status = job.get_reducerStatus();
		terminate_mappers(jobID, mapper_status);
		terminate_reducers(jobID, reducer_status);
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JobTrackerImpl jt = new JobTrackerImpl();
		String port = args[0];
		jt.init(port);
	}

	@Override
	public void print() throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("This is JobTracker");
	}
	
}
