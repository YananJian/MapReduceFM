package mr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.*;

import dfs.DataNode;
import dfs.NameNode;
import mr.common.Constants.TASK_STATUS;
import mr.common.Constants.TASK_TP;
import mr.io.TextWritable;
import mr.io.Writable;

/**
 * Task records all status of a task
 * @author Yanan Jian
 * @author Erdong Li
 */
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
    NameNode namenode = null;
    String read_from_machine = null;
    Registry dfs_registry = null;

    /**
     * Constructor
     * @param job_id job's id
     * @param task_id task's id
     * @param host host of DFS Registry
     * @param port port of DFS Registry
     */
    public Task(String job_id, String task_id, String host, int port) {
        this.job_id = job_id;
        this.task_id = task_id;
        try {
            dfs_registry = LocateRegistry.getRegistry(host, port);
            namenode = (NameNode) dfs_registry.lookup("NameNode");
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * set machineID to read from
     * @param machineID
     */
    public void set_read_from_machine(String machineID)
    {
        this.read_from_machine = machineID;
    }

    /**
     * set blockID
     * @param bID
     */
    public void set_blockID(String bID)
    {
        this.block_id = bID;
    }
    
    /**
     * set output DIR
     * @param dir
     */
    public void set_outputdir(String dir)
    {
        this.output_dir = dir;
    }

    /**
     * set task type, task type includes MAPPER, REDUCER
     * @param tp
     */
    public void set_taskTP(TASK_TP tp)
    {
        this.type = tp;
    }

    /**
     * set reducer number
     * @param rCT
     */
    public void set_reducerCT(int rCT)
    {
        this.reducer_ct = rCT;
    }

    /**
     * set mapper class
     * @param mapper
     */
    public void set_mapper_cls(Class<? extends Mapper> mapper) {
        this.mapper = mapper;
    }

    /**
     * set reducer class
     * @param reducer
     */
    public void set_reducer_cls(Class<? extends Reducer> reducer){
        this.reducer = reducer;
    }
    
    /**
     * set dir to read from
     * @param dir
     */
    public void set_read_dir(String dir) {
        this.read_dir = dir;
    }

    /**
     * set machineID
     * @param m_id
     */
    public void set_machineID(String m_id)
    {
        this.machine_id = m_id;
    }

    /**
     * overrided thread function, when the task starts, 
     * perform different actions based on task type.
     * If it is mapper task, after reading blocks, 
     * call the overrided map function,
     * partition and sort
     * If it is reducer task, after init and bootstrap reducer,
     * call the overrided reduce function,
     * 
     */
    @Override
    public Object call() throws RemoteException {
        try {
            if (Thread.interrupted())
                return TASK_STATUS.TERMINATED;
            if (type == TASK_TP.MAPPER)
            {
                System.out.println("------------Starting Mapper task in TaskTracker");
                Mapper<Object, Object, Object, Object> mapper_cls = mapper.newInstance();
                String output_tmpdir = "/tmp/"+job_id+'/'+machine_id+'/';
                Context context = new Context(job_id, task_id, reducer_ct, output_tmpdir, TASK_TP.MAPPER);
                System.out.println("Executing task, job id:" + job_id + ", mapper_id:" + task_id);
                /* read from block */
                DataNode dNode = namenode.getDataNode(Integer.parseInt(read_from_machine));
                String content = dNode.getBlock(Integer.valueOf(block_id));
                String[] lines = content.split("\\n");
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
                Context context = new Context(job_id, task_id, reducer_ct, output_dir, TASK_TP.REDUCER);
                System.out.println("Executing task, job id:" + job_id
                    + ", reducer_id:" + task_id);
                String partition_id = task_id.split("_r_")[1].trim();
                String input_dir = "/tmp/"+job_id+'/'+machine_id+'/';
                reducer_cls.init(input_dir);
                reducer_cls.bootstrap(partition_id);
                System.out.println("After bootstrap");
                Record record = null;
                while ((record = reducer_cls.getNext()) != null) {
                      TextWritable key = (TextWritable) record.getKey();
                      Iterable<Writable> values = (Iterable<Writable>) record.getValues();
                      reducer_cls.reduce(key, values, context);                      
                }
                return context.getContents();
            }
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
