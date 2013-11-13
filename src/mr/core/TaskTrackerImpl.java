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
import java.io.StringReader;
import java.lang.reflect.ParameterizedType;

import conf.Config;
import dfs.FileDownloader;
import dfs.FileUploader;
import dfs.NameNode;
import mr.Context;
import mr.Mapper;
import mr.Record;
import mr.Reducer;
import mr.Task;
import mr.common.Constants.MSG_TP;
import mr.common.Constants.TASK_STATUS;
import mr.common.Constants.TASK_TP;
import mr.common.Msg;
import mr.io.TextWritable;
import mr.io.Writable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TaskTrackerImpl implements TaskTracker, Callable{
	
	String registryHost = Config.MASTER_IP;
	int mrPort = Config.MR_PORT;
	int dfsPort = Config.DFS_PORT;
	JobTracker jobTracker = null;
	int id = 0;
	Registry mr_registry = null;
	String read_dir = null;
	ExecutorService exec = null;
	int reducer_ct = 0;
	int port = 0;
	//Queue<Msg> heartbeats = new LinkedList<Msg>();
	LinkedBlockingQueue<Msg> heartbeats = new LinkedBlockingQueue<Msg>();
	HashMap<String, Future> taskID_exec = new HashMap<String, Future>();
	public TaskTrackerImpl(int id, String read_dir, String port, int reducer_ct)
	{
		this.id = id;		
		try {
			mr_registry = LocateRegistry.getRegistry(registryHost, mrPort);
			this.jobTracker = (JobTracker) mr_registry.lookup("JobTracker");
			
			this.read_dir = read_dir;
			this.port = Integer.valueOf(port);
			this.reducer_ct = reducer_ct;
			exec = Executors.newCachedThreadPool();
			exec.submit(this);
			
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void terminate(String taskID)
	{
		Future f = taskID_exec.get(taskID);
		f.cancel(true);
		System.out.println("Task "+taskID+" terminated");
	}
	
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	public List<String> read_dir(String path, String hashID)
	{
		File dir = new File(path);
		File[] files = dir.listFiles();
		List<String> ret_names = new ArrayList<String>();
		for (int i=0; i< files.length; i++)
		{
			String name = files[i].getName();
			System.out.println("Name:"+name+", machineID:"+id);
			String tmp[] = name.split("@");
			if (tmp.length > 0)
				if (tmp[tmp.length-1].equals(hashID))
				{
					System.out.println("Filtered Name:"+name+", machineID:"+id);
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
			content.deleteCharAt(content.length()-1);
		    return content.toString();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return content.toString();
	}
	
	public void writestr(String path, String content)
	{
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(path, true));
			//FileOutputStream file_out=new FileOutputStream(path,true);
			out.write(content+'\n');		
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	@Override
	public void start_map(String job_id, String mapper_id, String block_id, String read_from_machine, Class<? extends Mapper> mapper) {
		// TODO Auto-generated method stub
		//mapper.map(key, val, context);
		Task task = new Task(job_id, mapper_id);
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
		
			//HashMap<String, Integer> idSize = (HashMap<String, Integer>) f1.get();
			
			/*
			 * After executing task, wrap the return value into Heartbeat Msg.
			 * Send Heartbeat to JobTracker.
			 * Per Msg per Task.
			 * 
			 * */
			Msg msg = new Msg();
			msg.setJob_id(job_id);
			msg.setTask_id(mapper_id);
			msg.setTask_tp(TASK_TP.MAPPER);
			//msg.setTask_stat(TASK_STATUS.FINISHED);
			msg.setTask_stat(TASK_STATUS.RUNNING);
			msg.set_future(f1);
			//msg.setContent(idSize);
			System.out.println("Finished Mapper, machineID:"+id+"\ttaskID:"+mapper_id);
			msg.setMachine_id(String.valueOf(id));
			System.out.println("ADDING HEARTBEAT MSG INTO QUEUE");
			this.heartbeats.offer(msg);
			
		
		
	}

	
	public void start_reducer(String job_id, String reducer_id, String write_path, Class<? extends Reducer> reducer)
	{
		Task task = new Task(job_id, reducer_id);
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

	@Override
	public void set_reducer_ct(int ct) {
		// TODO Auto-generated method stub
		this.reducer_ct = ct;
	}

	private void check_mapper(Msg msg)
	{
		Future f = msg.get_future();
		if (f != null)
		    if(f.isDone())
			{
				try {
					HashMap<String, Integer> idSize = (HashMap<String, Integer>) f.get();
					msg.set_future(null);
					msg.setContent(idSize);
					msg.setTask_stat(TASK_STATUS.FINISHED);
					jobTracker.heartbeat(msg);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		    else if (f.isCancelled())
		    {
		    	msg.setTask_stat(TASK_STATUS.TERMINATED);
		    }
		    else
		    {
		    	this.heartbeats.add(msg);
		    }
	}
	
	private void check_reducer(Msg msg)
	{
		String ret_s = "";
		Future f1 = msg.get_future();
		String reducer_id = msg.getTask_id();
		String output_path = msg.getOutput_path();
		if (f1 != null)
			if (f1.isDone())
			{
				LinkedList<Record> contents;
				try {
					contents = (LinkedList<Record>) f1.get();
				msg.set_future(null);
				Iterator<Record> iter = contents.iterator();
				while(iter.hasNext())
				{
					Record r = iter.next();
					String key = (String)r.getKey().getVal();
					Iterable<Writable> vals = r.getValues();
					for(Writable val: vals)
					{
						String s = key + "\t" + val.getVal().toString() +"\n";
						ret_s += s;
					}				
				}
				StringReader strr = new StringReader(ret_s);
				BufferedReader br = new BufferedReader(strr);
				FileUploader uploader = new FileUploader(br, reducer_id , 0, registryHost, dfsPort);
				uploader.upload();
				
				//FileDownloader downloader = new FileDownloader(write_path+'/', reducer_id, registryHost, dfsPort);
				FileDownloader downloader = new FileDownloader(output_path + '/' + reducer_id, reducer_id, registryHost, dfsPort);
				downloader.download();
				System.out.println("Writing to DFS, REDUCER ID:"+reducer_id);
				msg.setTask_stat(TASK_STATUS.FINISHED);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				} catch (NotBoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			 else if (f1.isCancelled())
			    {
			    	msg.setTask_stat(TASK_STATUS.TERMINATED);
			    }
			    else
			    {
			    	this.heartbeats.add(msg);
			    }
		
	}
	
	@Override
	public Object call() throws RemoteException, InterruptedException {
		// TODO Auto-generated method stub
		while(true)
		{
			Msg msg = this.heartbeats.poll();			
			if (msg != null)
			{
				if (msg.getTask_tp() == TASK_TP.MAPPER)
					check_mapper(msg);
				else if (msg.getTask_tp() == TASK_TP.REDUCER)
					check_reducer(msg);
				
				//System.out.println("Msg tp:"+msg.getMsg_tp()+",CPUs:"+msg.get_aval_procs());
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
		// TODO Auto-generated method stub
		
		
	}
	
	public static void main(String[] args) {
		
		// TaskTracker ID should be the same with the id of the DataNode
		String taskNodeID = args[0];
		String dir = args[1];
		String port = args[2];
		int reducer_ct = Integer.valueOf(args[3]);
		TaskTrackerImpl tt = new TaskTrackerImpl(Integer.parseInt(taskNodeID), dir, port, reducer_ct);
		tt.init();
	}
	
}
