package mr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.*;

import conf.Config;
import dfs.DataNode;
import dfs.NameNode;
import mr.common.Constants.TASK_TP;
import mr.io.IntWritable;
import mr.io.TextWritable;
import mr.io.Writable;

public class Task implements Callable {
	Class<? extends Mapper> mapper = null;
	Class<? extends Reducer> reducer = null;
	String job_id = null;
	String task_id = null;
	String read_dir = null;
	String block_id = null;
	String machine_id = null;
	int reducer_ct = 0;
	TASK_TP type = null;
	String output_dir = null;
	String registryHost = Config.MASTER_IP;
	int dfsPort = Config.DFS_PORT;
	NameNode namenode = null;
	String read_from_machine = null;
	Registry dfs_registry = null;
	
	public Task(String job_id, String task_id) {
		this.job_id = job_id;
		this.task_id = task_id;	
		
		try {
			dfs_registry = LocateRegistry.getRegistry(registryHost, dfsPort);
			namenode = (NameNode) dfs_registry.lookup("NameNode");
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}
	
	public void set_read_from_machine(String machineID)
	{
		this.read_from_machine = machineID;	
	}
	
	public void set_blockID(String bID)
	{
		this.block_id = bID;
	}
	public void set_outputdir(String dir)
	{
		this.output_dir = dir;
	}
	
	public void set_taskTP(TASK_TP tp)
	{
		this.type = tp;
	}
	
	public void set_reducerCT(int rCT)
	{
		this.reducer_ct = rCT;
	}
	public void set_mapper_cls(Class<? extends Mapper> mapper) {
		this.mapper = mapper;
	}

	public void set_reducer_cls(Class<? extends Reducer> reducer){
		this.reducer = reducer;
	}
	public void set_read_dir(String dir) {
		this.read_dir = dir;
	}

	public void set_machineID(String m_id)
	{
		this.machine_id = m_id;
	}

	@Override
	public Object call() throws RemoteException {
		// TODO Auto-generated method stub
		try {
			if (type == TASK_TP.MAPPER)
			{
				System.out.println("------------Starting Mapper task in TaskTracker");
				Mapper<Object, Object, Object, Object> mapper_cls = mapper.newInstance();
				String output_tmpdir = "tmp/"+job_id+'/'+machine_id+'/';
				Context context = new Context(job_id, task_id, reducer_ct, output_tmpdir);
				System.out.println("Executing task, job id:" + job_id
					+ ", mapper_id:" + task_id);
				/* read from block */
				//DataNode datanode = (DataNode) registry.lookup("DataNode_"+read_from_machine);
				DataNode dNode = namenode.getDataNode(Integer.parseInt(read_from_machine));
				String content = dNode.getBlock(Integer.valueOf(block_id));
				//BufferedReader br = new BufferedReader(new FileReader(read_dir
				//										+ "/" + block_id));
				//String line = "";
				String[] lines = content.split("\n");
				//while ((line = br.readLine()) != null) 
				for (int i= 0; i< lines.length; i++)
				{
					String line = lines[i];
					TextWritable k1 = new TextWritable();
					TextWritable v1 = new TextWritable();
					String k1_val = line;
					String v1_val = line;					
										
					k1.setVal(k1_val);
					v1.setVal(v1_val);					
										
					mapper_cls.map(k1, v1, context);
				}
				context.partition();
				return context.get_idSize();
			}
			else if (type == TASK_TP.REDUCER)
			{
				System.out.println("------------Starting Reducer task in Task");
				Reducer<Object, Object, Object, Object> reducer_cls = reducer.newInstance();
				//String output_tmpdir = "tmp/"+job_id+'/'+machine_id+'/';
				Context context = new Context(job_id, task_id, reducer_ct, output_dir);
				System.out.println("Executing task, job id:" + job_id
					+ ", reducer_id:" + task_id);
				String input_dir = "tmp/"+job_id+'/'+machine_id+'/';
				//System.out.println("Input to reducer, dir:"+input_dir);
				reducer_cls.init(input_dir);
				reducer_cls.bootstrap();
				System.out.println("After bootstrap");
				Record record = null;
				while ((record = reducer_cls.getNext()) != null) {
					  TextWritable key = (TextWritable) record.getKey();
				      //System.out.println("After bootrap, key:"+key.getVal());
				      Iterable<Writable> values = (Iterable<Writable>) record.getValues();
				      reducer_cls.reduce(key, values, context);				      
				}
				return context.get_Contents();
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
}
