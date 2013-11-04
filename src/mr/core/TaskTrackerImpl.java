package mr.core;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.lang.reflect.ParameterizedType;

import conf.Config;
import dfs.NameNode;
import mr.Context;
import mr.Mapper;
import mr.io.TextWritable;
import mr.io.Writable;

public class TaskTrackerImpl implements TaskTracker{
	
	String registryHost = Config.MASTER_IP;
	int registryPort = Config.MASTER_PORT;
	JobTracker jobTracker = null;
	int id = 0;
	Registry registry = null;
	public TaskTrackerImpl(int id)
	{
		this.id = id;		
		try {
			registry = LocateRegistry.getRegistry(registryHost, registryPort);
			this.jobTracker = (JobTracker) registry.lookup("JobTracker");
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void init()
	{
		try {	        	        
	        TaskTracker stub = (TaskTracker) UnicastRemoteObject.exportObject(this, 0);
	        registry.rebind("TaskTracker_"+String.valueOf(this.id), stub);	        
	        System.out.println("Registered");
	       
	      } catch (Exception e) {
	        
	        e.printStackTrace();
	        
	      }
	}
	@Override
	public void start_map(String job_id, String mapper_id, Class<? extends Mapper> mapper) {
		// TODO Auto-generated method stub
		//mapper.map(key, val, context);
		try {
			System.out.println("------------Starting Mapper task in TaskTracker");
					
			Mapper<Object, Object, Object, Object> mapper_cls = mapper.newInstance();
			
			Context context = new Context(job_id, mapper_id);
			
			System.out.println("Executing task, job id:"+job_id+", mapper_id:"+mapper_id);
			/* HARD CODING TEXTWRITABLE AS TYPE....*/
			TextWritable k1 = new TextWritable(); 
			TextWritable v1 = new TextWritable();
			String k1_val = "mapping, key";
			String v1_val = "mapping, val";
			k1.setVal(k1_val);
			v1.setVal(v1_val);
			
			mapper_cls.map(k1, v1, context);
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Starting Mapper");
		
	}

	public static void main(String[] args) {
		
		// TaskTracker ID should be the same with the id of the DataNode
		String taskNodeID = args[0];
		TaskTrackerImpl tt = new TaskTrackerImpl(Integer.parseInt(taskNodeID));
		tt.init();
	}
}
