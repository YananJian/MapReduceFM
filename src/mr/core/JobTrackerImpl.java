package mr.core;

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
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Set;

import mr.Job;
import mr.JobStatus;
import mr.Reducer;
import mr.common.Constants.*;
import mr.common.Msg;
import dfs.NameNode;


public class JobTrackerImpl implements JobTracker, Callable {
    String registryHost = null;
    int mrPort = 0;
    int dfsPort = 0;
    NameNode namenode = null;
    int reducer_ct = 0;
    boolean terminated = false;
    Hashtable<String, JobStatus> job_status = new Hashtable<String, JobStatus>();
    /**
     * mapper_machine: pair of job, mapperID and machineID
     * running_jobs: pair of machineID, list of running jobIDs
     *
     */
    Hashtable<String, Hashtable<String, String>> mapper_machine = new Hashtable<String, Hashtable<String, String>>();
    Hashtable<String, Hashtable<String, String>> reducer_machine = new Hashtable<String, Hashtable<String, String>> ();

    /* JobID, MachineID, partitionID, size */
    HashMap<String, HashMap<String, HashMap<String, Integer>>> job_mc_hash_size= new HashMap<String, HashMap<String, HashMap<String, Integer>>>();    
    HashMap<String, String> jobID_outputdir = new HashMap<String, String>();
    HashMap<String, Job> jobID_Job = new HashMap<String, Job>();
    HashMap<String, TaskTracker> alive_tasktrackers = new HashMap<String, TaskTracker>();
    HashMap<String, Set<String>> running_jobs = new HashMap<String, Set<String>>();
    HashMap<String, Integer> cpu_resource = new HashMap<String, Integer>();
    Registry mr_registry = null;
    Registry dfs_registry = null;
    ExecutorService exec = null;

    /**
     * Init JobTracker, create mrRegistry
     * @param registryHost mapreducefm's Registry host address
     * @param mrPort mapreducefm's registry port number
     * @param dfsPort dfs's port number
     * @param selfPort mapreducefm's port number
     * @param reducer_ct reducer numbers customized by client
     * @throws RemoteException
     */
    public void init(String registryHost, String mrPort, String dfsPort, String selfPort, String reducer_ct)
    {
        try {
            this.mrPort = Integer.valueOf(mrPort);
            this.dfsPort = Integer.valueOf(dfsPort);
            this.registryHost = registryHost;
            this.reducer_ct = Integer.valueOf(reducer_ct);
            mr_registry = LocateRegistry.createRegistry(this.mrPort);
            dfs_registry = LocateRegistry.getRegistry(this.registryHost, this.dfsPort);
            JobTracker stub = (JobTracker) UnicastRemoteObject.exportObject(this, Integer.valueOf(selfPort));
            mr_registry.rebind("JobTracker", stub);
            this.namenode = (NameNode) dfs_registry.lookup("NameNode");
            exec = Executors.newCachedThreadPool();
            exec.submit(this);
          } catch (Exception e) {
            e.printStackTrace();
          }
    }

    /**
     * preset_reducer:
     * Sum the sizes of partitions given back by mapper tasks,
     * Choose the Top N (N = num of reducers) sizes, for each partition,
     * allocate reducers on the machine that has most resources.
     *
     * Eg. We have 2 reducers.
     * Machine A has partition k1 = 1KB, k2 = 1MB
     * Machine B has partition k1 = 1MB, k2 = 1GB
     * Machine C has partition k1 = 1GB, k2 = 1TB
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
        /* allocate #reducer_ct reducers */
        for (int i = 0; i < reducer_ct; i++) {
            TreeMap<Integer, String> priorityQ = new TreeMap<Integer, String>();
            /* insert each record into a priority queue */
            for (Entry<String, HashMap<String, Integer>> entry : mc_hash_size.entrySet())
            {
                System.out.println("Entry:"+entry.toString());
                Integer size = entry.getValue().get(String.valueOf(i));
                if (size == null)
                    size = 0;
                priorityQ.put(size, entry.getKey());
                System.out.println("PriorityQ:"+priorityQ.toString());
            }
            /* iteratively get the machine with most records, and check #cpu available */
            Entry<Integer, String> entry = null;
            String machineId = "";
            boolean allocated = false;
            while ((entry = priorityQ.pollLastEntry()) != null) {
                machineId = entry.getValue();
                int availCPUs = cpu_resource.get(machineId);
                if (availCPUs > 0) {
                    availCPUs--;
                    cpu_resource.put(machineId, availCPUs);
                    allocated = true;
                    break;
                }
            }
            if (!allocated) {
                /* no idle machine found */
                String id = pick_idle_machine();
                if (!id.equals("")) {
                    machineId = id;
                    int availCPUs = cpu_resource.get(machineId);
                    availCPUs--;
                    cpu_resource.put(machineId, availCPUs);
                }
            }
            List<String> lst = partition_res.get(machineId);
            if (lst == null)
                lst = new ArrayList<String>();
            lst.add(String.valueOf(i));
            partition_res.put(machineId, lst);
        }
        System.out.println("Partition_res:"+partition_res.toString());
        return partition_res;
    }

    /**
     * Allocate mapper tasks to alive tasktrackers
     * @param machineID the ID of machine which alive tasktracker is running on
     * @param mapper_id ID of mapper
     * @param blockID ID of block
     * @param read_from_machine the machineID of which block is stored and to be read by tasktracker. 
     * @param job job which has to be scheduled
     * @param mc_mp hashmap of machineID and mapper_id
     */
    public void allocate_mapper(String machineID, String mapper_id, String blockID, String read_from_machine, Job job, Hashtable<String, String> mc_mp)
    {
        TaskTracker tt;
        try {
            tt = this.alive_tasktrackers.get(machineID);
            /**
             * TaskTracker has to hash key to an ID based on REDUCER_NUM 
             * Thus to ensure same key on different mappers will have the same ID
             * So finally same key will go to the same reducer.
             * After Context iterated over all the keys, (Thus after the first stage of partition)
             * push the {key:id} hashmap to TaskTracker, TaskTracker then pass it to JobTracker.
             * JobTracker then coordinate which ID goes to which reducer (locality considered).
             *
             */
            tt.set_reducer_ct(reducer_ct);
            mc_mp.put(mapper_id, machineID);
            mapper_machine.put(job.get_jobId(), mc_mp);
            System.out.println("prepare to start mapper");
            tt.start_map(job.get_jobId(), mapper_id, blockID, read_from_machine, job.get_mapper_cls(), job.get_mapper_clspath());
            job.set_mapperStatus(mapper_id, TASK_STATUS.RUNNING);
            job.inc_mapperct();
            jobID_Job.put(job.get_jobId(), job);
            System.out.println("Job Tracker trying to start map task on "+String.valueOf(machineID)
                                                                            +", mapperID:"+mapper_id);
        } catch (AccessException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    public String pick_idle_machine()
    {
        Iterator<Entry<String, Integer>> iter = this.cpu_resource.entrySet().iterator();
        String arbi = "";
        while(iter.hasNext())
        {
            Entry<String, Integer> entry = (Entry<String, Integer>) iter.next();
            String machineID = (String)entry.getKey();
            if (!this.alive_tasktrackers.containsKey(machineID))
                continue;
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
            job.set_jobStatus(JOB_STATUS.RUNNING);
            jobID_Job.put(job.get_jobId(), job);
            Set<?> set = mappings.entrySet();
            if (job.get_fileOutputPath() != null)
                jobID_outputdir.put(job.get_jobId(), job.get_fileOutputPath());
            System.out.println("Scheduling Job "+job.get_jobId());
            Hashtable<String, String> mc_mp = new Hashtable<String, String>();
            for(Iterator<?> iter = set.iterator(); iter.hasNext();)
              {
               @SuppressWarnings("rawtypes")
               Entry entry = (Entry)iter.next();
               Integer blockID = (Integer)entry.getKey();
               @SuppressWarnings("unchecked")
               List<Integer> value = (List<Integer>)entry.getValue();
               System.out.println("BlockID:"+blockID.toString());
               /* set mapper task */
               String mapper_id = job.get_jobId() + "_m_" + String.valueOf(blockID);
               boolean allocated = false;
               for (Integer machineID: value)
               {
                   if (!this.alive_tasktrackers.containsKey(String.valueOf(machineID)))
                       continue;
                   System.out.println("MachineID:"+machineID.toString());
                   int aval_cpus = this.cpu_resource.get(String.valueOf(machineID));
                   System.out.println("Aval CPU NUM:"+aval_cpus);
                   if (aval_cpus > 0)
                   {
                       System.out.println("Availble CPU, machine: "+machineID);
                       String read_from_machine = String.valueOf(machineID);
                       allocate_mapper(String.valueOf(machineID), mapper_id, String.valueOf(blockID), read_from_machine, job, mc_mp);
                       aval_cpus --;
                       this.cpu_resource.put(String.valueOf(machineID), aval_cpus);
                       allocated = true;
                       Set<String> running_jobIDs = running_jobs.get(machineID);
                       if (running_jobIDs == null)
                       {
                           running_jobIDs = new HashSet<String>();
                           running_jobIDs.add(job.get_jobId());
                           running_jobs.put(String.valueOf(machineID), running_jobIDs);
                       }
                       else
                       {
                           running_jobIDs.add(job.get_jobId());
                       }
                       break;
                   }
               }
               if (!allocated)
               {
                   String machineID = pick_idle_machine();
                   if (!machineID.equals(""))
                   {
                   int aval_cpus = this.cpu_resource.get(machineID);
                   String read_from_machine = String.valueOf(value.get(0));
                   allocate_mapper(machineID, mapper_id, String.valueOf(blockID), read_from_machine, job, mc_mp);
                   aval_cpus --;
                   this.cpu_resource.put(machineID, aval_cpus);
                   allocated = true;
                   Set<String> running_jobIDs = running_jobs.get(machineID);
                       if (running_jobIDs == null)
                       {
                       running_jobIDs = new HashSet<String>();
                       running_jobIDs.add(job.get_jobId());
                       running_jobs.put(machineID, running_jobIDs);
                       }
                       else
                       {
                       running_jobIDs.add(job.get_jobId());
                       }
                   }
                   else
                   {
                       System.out.println("No Available Machine");
                   }
               }
              }
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the machine IDs of the given job's mappers
     * @param jobID job's id
     * @return a set of machine IDs
     */
    public Set<String> get_mapper_machineIDs(String jobID)
    {
        Hashtable<String, String> mp_mc = mapper_machine.get(jobID);
        Iterator<Entry<String, String>> iter = mp_mc.entrySet().iterator(); 
        Set<String> mcIDs = new HashSet<String>();
        while (iter.hasNext())
        {
            Entry<String, String> entry = (Entry<String, String>) iter.next(); 
            String val = (String) entry.getValue();
            mcIDs.add(val);
        }
        return mcIDs;
    }

    public void shuffle(String jobID, HashMap<String, List<String>> mcID_hashIDs)
    {
        System.out.println("Shuffling ....");
        Iterator<Entry<String, List<String>>> iter = mcID_hashIDs.entrySet().iterator();
        Set<String> mcIDsets = get_mapper_machineIDs(jobID);
        while (iter.hasNext()) {
            Iterator<String> mc_iter = mcIDsets.iterator();
            Entry<String, List<String>> entry = (Entry<String, List<String>>) iter.next();
            String machineID = (String) entry.getKey();             
            List<String> hashIDs = (List<String>) entry.getValue();
            try {
                TaskTracker w_taskTracker = this.alive_tasktrackers.get(machineID);
                String w_path = "/tmp/" + jobID + '/' + machineID + '/';
                while(mc_iter.hasNext())
                {
                    String curr = mc_iter.next().toString();
                    if (curr.equals(machineID))
                        continue;
                    TaskTracker r_taskTracker = this.alive_tasktrackers.get(curr);
                    String r_path = "/tmp/" + jobID + '/' + curr + '/';
                    for (int i = 0;i< hashIDs.size(); i++)
                    {
                        List<String> names = r_taskTracker.readDIR(r_path+'/', hashIDs.get(i));
                        for (int j = 0; j < names.size(); j++)
                        {
                            String content = r_taskTracker.readstr(r_path+'/', names.get(j));
                            w_taskTracker.writestr(w_path+'/'+names.get(j), content);
                            System.out.println("Wrote to path:"+w_path+'/'+names.get(j));
                        }
                    }
                }
            } catch (AccessException e) {
                e.printStackTrace();
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }

    public void start_reducer(String job_id, String write_path, HashMap<String, List<String>> mcID_hashIDs)
    {
        Iterator<Entry<String, List<String>>> iter = mcID_hashIDs.entrySet().iterator();
        while(iter.hasNext())
        {
            Entry<String, List<String>> entry = (Entry<String, List<String>>) iter.next();
            String mcID = (String)entry.getKey();
            List<String> hashIDs = (List<String>)entry.getValue();
            TaskTracker tt = this.alive_tasktrackers.get(mcID);
            for (String id : hashIDs) {
                String reducer_id = job_id + "_r_" + id;
                Job job = this.jobID_Job.get(job_id);
                Class<? extends Reducer> reducer = job.get_reducer_cls();
                String cls_path = job.get_reducer_clspath();
                try {
                    tt.start_reducer(job_id, reducer_id, write_path, reducer, cls_path);
                    Hashtable<String, String> rcmc = new Hashtable<String, String>();
                    rcmc.put(reducer_id, mcID);
                    reducer_machine.put(job_id, rcmc);
                    job.inc_reducerct();
                    job.set_reducerStatus(reducer_id, TASK_STATUS.RUNNING);
                    System.out.println("Starting Reducer in JobTracker, job_id:"+job_id+", reducer id:"+reducer_id);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public boolean is_task_finished(String jobID, TASK_TP tp)
    {
        System.out.println("Checking ------------------------");
        Job job = jobID_Job.get(jobID);
        HashMap<String,TASK_STATUS> task_status = null;
        if (tp == TASK_TP.MAPPER)
            task_status = job.get_mapperStatus();
        else if (tp == TASK_TP.REDUCER)
            task_status = job.get_reducerStatus();
        Iterator<Entry<String, TASK_STATUS>> iter = task_status.entrySet().iterator();
        boolean finished = true;
        while(iter.hasNext())
        {
            Entry<String, TASK_STATUS> pairs = (Entry<String, TASK_STATUS>) iter.next();
            String mapperID = (String)pairs.getKey();
            TASK_STATUS status = (TASK_STATUS)pairs.getValue();
            if (status == TASK_STATUS.RUNNING)
            {
                System.out.println("Task "+mapperID+" is running");
                finished = false;
            }
            else if (status == TASK_STATUS.FINISHED)
                System.out.println("Task "+mapperID+" is finished");
            else if (status == TASK_STATUS.TERMINATED)
            	System.out.println("Task "+mapperID+" is terminated");
        }
        return finished;
    }

    private void restart_jobs(String machineID)
    {
        System.out.println("--------In restart jobs");
        Set<String> running_jobIDs = running_jobs.get(machineID);
        System.out.println("Running jobIDs on broken machine "+machineID+":"+running_jobIDs.toString());
        for(String jobID: running_jobIDs)
        {
            Job job = this.jobID_Job.get(jobID);
            HashMap<String, HashMap<String, Integer>> mc_hash_size = job_mc_hash_size.get(jobID);
            if (mc_hash_size != null)
                mc_hash_size.remove(machineID);
            System.out.println("Terminating Job:"+jobID+" because of machine:"+machineID+" is down");
            terminate_job(jobID);
            System.out.println("Restarting Job:"+jobID+" because of machine:"+machineID+" is down");
            schedule(job);
        }
    }

    @Override
    public void heartbeat(Msg msg)
    {
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
                int aval_cpus = this.cpu_resource.get(machineID);
                aval_cpus++;
                cpu_resource.put(machineID, aval_cpus);
                
                HashMap<String, Integer> ret = (HashMap<String, Integer>)msg.getContent();
                HashMap<String, HashMap<String, Integer>> mc_hash_size = job_mc_hash_size.get(jobID);
                if (mc_hash_size == null)
                {
                    HashMap<String, HashMap<String, Integer>> f_mc_hash_size = new HashMap<String, HashMap<String, Integer>>();
                    f_mc_hash_size.put(machineID, ret);
                    job_mc_hash_size.put(jobID, f_mc_hash_size);
                }
                else
                {
                    mc_hash_size.put(machineID, ret);    
                }
                if (is_task_finished(jobID, TASK_TP.MAPPER))
                {
                    System.out.println("Mapper Job finished");
                    HashMap<String, List<String>> mcID_hashIDs = preset_reducer(jobID);
                    System.out.println("Before Shuffle, mcID_hashIDs:"+mcID_hashIDs.toString());
                    shuffle(jobID, mcID_hashIDs);
                    System.out.println("After Shuffle, mcID_hashIDs:"+mcID_hashIDs.toString());
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
                String machineID = msg.getMachine_id();
                Job job = jobID_Job.get(jobID);
                int aval_cpus = this.cpu_resource.get(machineID);
                aval_cpus++;
                cpu_resource.put(machineID, aval_cpus);
                job.set_reducerStatus(taskID, TASK_STATUS.FINISHED);
                if (is_task_finished(jobID, TASK_TP.REDUCER))
                {
                    System.out.println("All Reducer finished");
                    job.set_jobStatus(JOB_STATUS.FINISHED);
                    remove_from_running_jobs(jobID);
                }
            }
        }
        if (msg.getMsg_tp() == MSG_TP.HEARTBEAT)
        {
            cpu_resource.put(msg.getMachine_id(), msg.get_aval_procs());
        }
    }

    @Override
    public String desc_job(String jobID) throws RemoteException
    {
        if (terminated)
            throw new RemoteException("JobTracker terminating");
        /* retrieve necessary data */
        Job job = jobID_Job.get(jobID);
        int nMapper = job.get_mapperct();
        int nReducer = job.get_reducerct();
        /*Map<String, TASK_STATUS> mapperStatus = job.get_mapperStatus();
        Map<String, TASK_STATUS> reducerStatus = job.get_reducerStatus();*/
        /* construct status msg */
        StringBuilder sb = new StringBuilder();
        sb.append("Job ID: " + jobID + "\n");
        sb.append("Input File: " + job.get_fileName() + "\n");
        sb.append("Output Path: " + job.get_fileOutputPath() + "\n");
        sb.append("Mapper: " + job.get_mapper_cls().getName() + "\n");
        sb.append("#Allocated Mapper Instance: " + nMapper + "\n");
        sb.append("Reducer: " + job.get_reducer_cls().getName() + "\n");
        sb.append("#Allocated Reducer Instance: " + nReducer + "\n");
        sb.append("Job status:" + job.get_jobStatus().toString());
        /*int nFinishedMapper = 0;
        for (Map.Entry<String, TASK_STATUS> entry : mapperStatus.entrySet())
            if (entry.getValue() == TASK_STATUS.FINISHED)
                nFinishedMapper++;
        float mapperProgress = 0;
        if (nMapper != 0)
          mapperProgress = nFinishedMapper/nMapper*100;
        int nFinishedReducer = 0;
        for (Map.Entry<String, TASK_STATUS> entry : reducerStatus.entrySet())
            if (entry.getValue() == TASK_STATUS.FINISHED)
                nFinishedReducer++;
        float reducerProgress = 0;
        if (nReducer != 0)
         reducerProgress =  nFinishedReducer/nReducer*100;
        sb.append("Progress: Mapper " + (int)mapperProgress + "%\tReducer: " + (int)reducerProgress + "%\n");
       */
        return sb.toString();
    }

    @Override
    public String desc_jobs() throws RemoteException
    {
        if (terminated)
              throw new RemoteException("JobTracker terminating");
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
        System.out.println("----------In terminate_mappers");
        Iterator<Entry<String, TASK_STATUS>> miter = mapper_status.entrySet().iterator();
        while(miter.hasNext())
        {
            Entry<String, TASK_STATUS> pairs = (Entry<String, TASK_STATUS>) miter.next();
            String mapperID = (String)pairs.getKey();
            String machineID = mapper_machine.get(jobID).get(mapperID);
            TASK_STATUS status = (TASK_STATUS)pairs.getValue();
            if (status == TASK_STATUS.RUNNING)
            {
                TaskTracker tt;
                try {
                    tt = this.alive_tasktrackers.get(machineID);
                    if (tt != null)
                    {
                        tt.terminate(mapperID);
                        System.out.println("Task "+mapperID +" is terminated");
                        this.jobID_Job.get(jobID).set_mapperStatus(mapperID, TASK_STATUS.TERMINATED);
                    }
                } catch (AccessException e) {
                    e.printStackTrace();
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
            else if (status == TASK_STATUS.FINISHED)
                System.out.println("Task "+mapperID+" is finished");
        }
    }

    private void terminate_reducers(String jobID, HashMap<String,TASK_STATUS> reducer_status)
    {
        System.out.println("----------In terminate_reducers");
        Iterator<Entry<String, TASK_STATUS>> riter = reducer_status.entrySet().iterator();
        while(riter.hasNext())
        {
            Entry<String, TASK_STATUS> pairs = (Entry<String, TASK_STATUS>) riter.next();
            String reducerID = (String)pairs.getKey();
            String machineID = reducer_machine.get(jobID).get(reducerID);
            TASK_STATUS status = (TASK_STATUS)pairs.getValue();
            if (status == TASK_STATUS.RUNNING)
            {
                TaskTracker tt;
                try {
                    tt = this.alive_tasktrackers.get(machineID);
                    if (tt != null)
                    {
                        tt.terminate(reducerID);
                        System.out.println("Task "+reducerID +" is terminated");
                        this.jobID_Job.get(jobID).set_reducerStatus(reducerID, TASK_STATUS.TERMINATED);
                    }
                } catch (AccessException e) {
                    e.printStackTrace();
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
            else if (status == TASK_STATUS.FINISHED)
                System.out.println("Task "+reducerID+" is finished");
        }
    }

    private void remove_from_running_jobs(String jobID)
    {
        Set<Entry<String, Set<String>>> set = running_jobs.entrySet();
        Iterator<Entry<String, Set<String>>> iter = set.iterator();
        while(iter.hasNext())
        {
            Entry<String, Set<String>> entry = (Entry<String, Set<String>>)iter.next();
            Set<String> jobIDs = (Set<String>)entry.getValue();
            for (String running_jobID: jobIDs)
            {
                if (running_jobID.equals(jobID))
                {
                    jobIDs.remove(running_jobID);
                }
            }
        }
    }

    public void terminate_job(String jobID)
    {
        System.out.println("--------In terminate Job");
        Job job = jobID_Job.get(jobID);
        HashMap<String,TASK_STATUS> mapper_status = null;
        HashMap<String,TASK_STATUS> reducer_status = null;
        mapper_status = job.get_mapperStatus();
        reducer_status = job.get_reducerStatus();
        terminate_mappers(jobID, mapper_status);
        terminate_reducers(jobID, reducer_status);
        job.set_jobStatus(JOB_STATUS.TERMINATED);
    }

    @Override
    public void print() throws RemoteException {
        System.out.println("This is JobTracker");
    }

    public void health_check()
    {
        while(true)
        {
        	if (this.terminated)
        		break;
            Set<Entry<String, TaskTracker>> set = alive_tasktrackers.entrySet();
            Iterator<Entry<String, TaskTracker>> iter = set.iterator();
            while(iter.hasNext())
            {
                Entry<String, TaskTracker> ent = (Entry<String, TaskTracker>) iter.next();
                String machineID = (String)ent.getKey();
                TaskTracker tt = (TaskTracker)ent.getValue();
                try {
                    tt.heartbeat();
                } catch (RemoteException e) {
                    System.out.println("Warning: TaskTracker on "+machineID+" has dead");
                    this.alive_tasktrackers.remove(machineID);
                    restart_jobs(machineID);
                }
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public Object call() {
        health_check();
        return null;
    }

    @Override
    public void register(String machineID, TaskTracker tt) throws RemoteException {
        alive_tasktrackers.put(machineID, tt);
    }

    private void terminate_allJobs()
    {
        Set<Entry<String, Job>> jobset = jobID_Job.entrySet();
        Iterator<Entry<String, Job>> iter = jobset.iterator();
        while(iter.hasNext())
        {
            Entry<String, Job> entry = (Entry<String, Job>)iter.next();
            String jobID = (String) entry.getKey();
            Job job = (Job) entry.getValue();
            if (job.get_jobStatus() == JOB_STATUS.RUNNING)
            {
            	System.out.println("Job "+job.get_jobId() + " is running, terminating it");
                terminate_job(jobID);
            }
        }
    }

    @Override
    public void terminate() throws RemoteException{
        terminated = true;
        terminate_allJobs();
        System.out.println("Teminated all running jobs");
        Set<Entry<String, TaskTracker>> s = alive_tasktrackers.entrySet();
        Iterator<Entry<String, TaskTracker>> iter = s.iterator();
        while(iter.hasNext())
        {
            Entry<String, TaskTracker> entry = (Entry<String, TaskTracker>)iter.next();
            TaskTracker tt = (TaskTracker)entry.getValue();
            System.out.println("Terminating TaskTracker ");
            tt.terminate_self();
        }
        try {
            mr_registry.unbind("JobTracker");
            UnicastRemoteObject.unexportObject(this, true);
            UnicastRemoteObject.unexportObject(mr_registry, true);
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
    }

    public void kill() throws RemoteException
    {
    	if (this.terminated)
    		return;
    	this.terminate_allJobs();
    }
    
    public void kill(String jobID) throws RemoteException
    {
    	if (this.terminated)
    		return;
    	this.terminate_job(jobID);
    }
    
    public static void main(String[] args) {
        JobTrackerImpl jt = new JobTrackerImpl();
        String registryHost = args[0];
        String mrPort = args[1];
        String dfsPort = args[2];
        String selfPort = args[3];
        String reducer_ct = args[4];
        jt.init(registryHost, mrPort, dfsPort, selfPort, reducer_ct);
    }
}
