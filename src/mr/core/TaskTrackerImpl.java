package mr.core;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import dfs.ClassDownloader;
import dfs.FileUploader;
import mr.Mapper;
import mr.Record;
import mr.Reducer;
import mr.Task;
import mr.common.Constants.MSG_TP;
import mr.common.Constants.TASK_STATUS;
import mr.common.Constants.TASK_TP;
import mr.common.Msg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TaskTrackerImpl implements TaskTracker, Callable{
    String registryHost = null;
    int mrPort = 0;
    int dfsPort = 0;
    JobTracker jobTracker = null;
    int id = 0;
    Registry mr_registry = null;
    String read_dir = null;
    ExecutorService exec = null;
    int reducer_ct = 0;
    int port = 0;
    boolean terminated = false;
    LinkedBlockingQueue<Msg> heartbeats = new LinkedBlockingQueue<Msg>();
    HashMap<String, Future> taskID_exec = new HashMap<String, Future>();

    /**
     * Constructor
     * @param registryHost host of registry, the MapReduceFM uses RMI for message passing
     * @param mrPort mapreduceFM's registry port
     * @param dfsPort DFS's port
     * @param selfPort this tasktracker's port
     * @param id this tasktracker's identifier
     * @param read_dir the directory that this tasktracker is going to read from
     * @param reducer_ct number of reducers
     */
    public TaskTrackerImpl(String registryHost, String mrPort, String dfsPort, String selfPort, int id, String read_dir, int reducer_ct)
    {
        this.id = id;
        try {
            this.registryHost = registryHost;
            this.mrPort = Integer.valueOf(mrPort);
            this.dfsPort = Integer.valueOf(dfsPort);
            mr_registry = LocateRegistry.getRegistry(registryHost, this.mrPort);
            this.jobTracker = (JobTracker) mr_registry.lookup("JobTracker");
            this.read_dir = read_dir;
            this.port = Integer.valueOf(selfPort);
            this.reducer_ct = reducer_ct;
            exec = Executors.newCachedThreadPool();
            exec.submit(this);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * terminate a task
     * @param taskID ID of the task
     */
    public void terminate(String taskID)
    {
        Future f = taskID_exec.get(taskID);
        f.cancel(true);
        System.out.println("Task "+taskID+" terminated");
    }

    /**
     * initiate process, export stub, send initiate message to JobTracker
     */
    public void init()
    {
        try {
            TaskTracker stub = (TaskTracker) UnicastRemoteObject.exportObject(this, port);
            jobTracker.register(String.valueOf(id), stub);
            System.out.println("Registered");
            Msg hb_msg = new Msg();
            Integer aval_procs = new Integer(Runtime.getRuntime().availableProcessors());
            System.out.println("Avalible CPU numbers:"+aval_procs.toString());
            hb_msg.set_aval_procs(aval_procs);
            hb_msg.setMsg_tp(MSG_TP.HEARTBEAT);
            hb_msg.setMachine_id(String.valueOf(id));
            this.heartbeats.offer(hb_msg);
          } catch (Exception e) {
            e.printStackTrace();
          }
    }

    public void writebr(String path, byte[]content)
    {
        try {
            FileOutputStream file_out=new FileOutputStream(path,true);
            file_out.write(content);
            file_out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * read all partitioned filenames from the specified directory based on the hashedID
     * @param path DIR to read from
     * @param hashID partitions are hashed based on reducer numbers, 
     * this ID determines what partitions should be processed by a specific task
     */
    public List<String> readDIR(String path, String hashID)
    {
        File dir = new File(path);
        File[] files = dir.listFiles();
        List<String> ret_names = new ArrayList<String>();
        for (int i=0; i< files.length; i++)
        {
            String name = files[i].getName();
            String tmp[] = name.split("@");
            if (tmp.length > 0)
                if (tmp[tmp.length-1].equals(hashID))
                {
                    ret_names.add(name);        
                }
        }
        return ret_names;
    }

    public String readstr(String path, String name)
    {
        StringBuilder content = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(path+'/'+name));
            String line = null;
            while ((line = br.readLine()) != null)
                content.append(line+'\n');
            br.close();
            File delf = new File(path + '/' + name);
            delf.delete();
            return content.toString();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content.toString();
    }

    public void writestr(String path, String content)
    {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(path, true));
            out.write(content+'\n');
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * start mapper task
     * @param job_id
     * @param mapper_id
     * @param block_id
     * @param read_from_machine machine to read block from 
     * @param mapper mapper Class
     * @param clspath path of mapper Class
     */
    @Override
    public void start_map(String job_id, String mapper_id, String block_id, String read_from_machine, Class<? extends Mapper> mapper, String clspath) {
        ClassDownloader dn = new ClassDownloader(clspath, clspath, registryHost, dfsPort);
        try {
            dn.download();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
        Task task = new Task(job_id, mapper_id, registryHost, dfsPort);
        task.set_taskTP(TASK_TP.MAPPER);
        task.set_blockID(block_id);
        task.set_reducerCT(reducer_ct);
        task.set_mapper_cls(mapper);
        task.set_read_dir(read_dir);
        task.set_mapper_cls(mapper);
        task.set_machineID(String.valueOf(id));
        task.set_read_from_machine(read_from_machine);
        Future f1 = exec.submit(task);
        taskID_exec.put(mapper_id, f1);
        /*
         * After executing task, wrap the return value into Heartbeat Msg.
         * Send Heartbeat to JobTracker.
         * Per Msg per Task
         */
        Msg msg = new Msg();
        msg.setJob_id(job_id);
        msg.setTask_id(mapper_id);
        msg.setTask_tp(TASK_TP.MAPPER);
        msg.setTask_stat(TASK_STATUS.RUNNING);
        msg.set_future(f1);
        msg.setMachine_id(String.valueOf(id));
        this.heartbeats.offer(msg);
    }

    /**
     * start reducer task
     * @param job_id
     * @param reducer_id
     * @param write_path path for writing outputs
     * @param reducer reducer Class
     * @param clspath path of reducer Class
     */
    public void start_reducer(String job_id, String reducer_id, String write_path, Class<? extends Reducer> reducer, String clspath)
    {
        ClassDownloader dn = new ClassDownloader(clspath, clspath, registryHost, dfsPort);
        try {
            dn.download();
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
        Task task = new Task(job_id, reducer_id, registryHost, dfsPort);
        task.set_taskTP(TASK_TP.REDUCER);
        task.set_reducer_cls(reducer);
        task.set_outputdir(write_path);
        task.set_reducerCT(reducer_ct);
        task.set_machineID(String.valueOf(id));
        Future f1 = exec.submit(task);
        taskID_exec.put(reducer_id, f1);


        Msg msg = new Msg();
        msg.setJob_id(job_id);
        msg.setTask_id(reducer_id);
        msg.setTask_tp(TASK_TP.REDUCER);
        msg.setTask_stat(TASK_STATUS.RUNNING);
        msg.setMachine_id(String.valueOf(id));
        msg.setOutput_path(write_path);
        msg.set_future(f1);
        this.heartbeats.offer(msg);
    }

    /**
     * set number of reducers
     */
    @Override
    public void set_reducer_ct(int ct) {
        this.reducer_ct = ct;
    }

    /**
     * mappers are asynchronous, this function checks if mapper is finished or terminated
     * @param msg
     */
    private void check_mapper(Msg msg)
    {
        Future f = msg.get_future();
        if (f != null)
        {
            if(f.isDone())
            {
                try {
                    if (f.get() == TASK_STATUS.TERMINATED)
                    {
                        msg.setTask_stat(TASK_STATUS.TERMINATED);
                        msg.set_future(null);
                        jobTracker.heartbeat(msg);
                    }
                    HashMap<String, Integer> idSize = (HashMap<String, Integer>) f.get();
                    msg.set_future(null);
                    msg.setContent(idSize);
                    msg.setTask_stat(TASK_STATUS.FINISHED);
                    jobTracker.heartbeat(msg);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
            else if (f.isCancelled())
            {
                msg.setTask_stat(TASK_STATUS.TERMINATED);
                msg.set_future(null);
                try {
                    jobTracker.heartbeat(msg);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
            else
            {
                this.heartbeats.add(msg);
            }
        }
    }

    /**
     * reducers are asynchronous, this function checks if reducer is finished or terminated
     * @param msg
     */
    private void check_reducer(Msg msg)
    {
        Future f1 = msg.get_future();
        String reducer_id = msg.getTask_id();
        String output_path = msg.getOutput_path();
        if (f1 != null)
        {
            if (f1.isDone())
            {
                LinkedList<Record> contents;
                try {
                    if (f1.get() != null)
                    {
                        if ( f1.get() == TASK_STATUS.TERMINATED)
                        {
                            msg.setTask_stat(TASK_STATUS.TERMINATED);
                            msg.set_future(null);
                            jobTracker.heartbeat(msg);
                        }
                    String path = output_path + '/' + reducer_id;
                    try {
                        FileUploader uploader = new FileUploader((String) f1.get(), path, 0, registryHost, dfsPort);
                        uploader.upload();
                        System.out.println("output path:"+path);
                        System.out.println("reducerID:"+reducer_id);
                        System.out.println("Writing to DFS, REDUCER ID:"+reducer_id);
                    } catch (IOException e) {
                        // ignore
                    }
                    msg.setTask_stat(TASK_STATUS.FINISHED);
                    msg.set_future(null);
                    jobTracker.heartbeat(msg);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (RemoteException e) {
                    e.printStackTrace();
                } catch (NotBoundException e) {
                    e.printStackTrace();
                }
            }
            else if (f1.isCancelled())
            {
                msg.setTask_stat(TASK_STATUS.TERMINATED);
                try {
                    msg.set_future(null);
                    jobTracker.heartbeat(msg);
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
            else
            {
                this.heartbeats.add(msg);
            }
        }
    }

    /**
     * thread function, send heartbeat message to JobTracker
     */
    @Override
    public Object call() throws RemoteException, InterruptedException {
        while(true)
        {
        	if (this.terminated)
        		return null;
            Msg msg = this.heartbeats.poll();
            if (msg != null)
            {
                if (msg.getTask_tp() == TASK_TP.MAPPER)
                    check_mapper(msg);
                else if (msg.getTask_tp() == TASK_TP.REDUCER)
                    check_reducer(msg);
                else
                    jobTracker.heartbeat(msg);
            }
            else
            {
                TimeUnit.SECONDS.sleep(1);
            }
        }
    }

    @Override
    public void heartbeat() throws RemoteException {
    }

    /**
     * terminate this TaskTracker
     */
    @Override
    public void terminate_self() throws RemoteException
    {
    	System.out.println("TaskTracker "+this.id + " is terminating");
    	this.terminated = true;
        UnicastRemoteObject.unexportObject(this, true);
        System.out.println("TaskTracker "+this.id + " terminated");
    }

    public static void main(String[] args) {
        /* TaskTracker ID should be the same with the id of the DataNode */
        String registryHost = args[0];
        String mrPort = args[1];
        String dfsPort = args[2];
        String selfPort = args[3];
        int id = Integer.valueOf(args[4]);
        String read_dir = args[5];
        int reducer_ct = Integer.valueOf(args[6]);
        TaskTrackerImpl tt = new TaskTrackerImpl(registryHost, mrPort, dfsPort, selfPort, id, read_dir, reducer_ct);
        tt.init();
    }
}
