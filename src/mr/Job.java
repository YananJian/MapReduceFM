package mr;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.UUID;

import mr.common.Constants.JOB_STATUS;
import mr.core.*;
import dfs.NameNode;
import mr.common.Constants.*;

/**
 * Job describes a MapReduce job
 * @author Yanan Jian
 * @author Erdong Li
 */
public class Job implements java.io.Serializable{
    private static final long serialVersionUID = 1L;
    Class<? extends Mapper> mapper = null;
    Class<? extends Reducer> reducer = null;
    String mapper_clspath = null;
    String reducer_clspath = null;
    String fileInputPath = null;
    String fileOutputPath = null;
    String fname = null;
    private Registry registry = null;
    NameNode namenode = null;
    JobTracker jobTracker = null;
    String job_id = null;
    int mapper_ct = 0;
    int reducer_ct = 0;
    HashMap<String, TASK_STATUS> mapper_status = new HashMap<String, TASK_STATUS>();
    HashMap<String, TASK_STATUS> reducer_status = new HashMap<String, TASK_STATUS>();
    JOB_STATUS stat = null;

    /**
     * Constructor
     * @param host registry's host name
     * @param port registry's port number
     */
    public Job(String host, int port)
    {
        try {
            registry = LocateRegistry.getRegistry(host, port);
            jobTracker = (JobTracker) registry.lookup("JobTracker");
            Long uuid = Math.abs(UUID.randomUUID().getMostSignificantBits());
            job_id = String.valueOf(uuid);
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (NotBoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Set the job's status
     * @param stat job's status
     */
    public void set_jobStatus(JOB_STATUS stat)
    {
        this.stat = stat;
    }

    /**
     * Get the job's status
     * @return job's status
     */
    public JOB_STATUS get_jobStatus()
    {
        return this.stat;
    }

    /**
     * Set mapper's status
     * @param mapper_id mapper's id
     * @param status mapper's status
     */
    public void set_mapperStatus(String mapper_id, TASK_STATUS status)
    {
        mapper_status.put(mapper_id, status);
    }

    /**
     * Set reducer's status
     * @param reducer_id reducer's id
     * @param status reducer's status
     */
    public void set_reducerStatus(String reducer_id, TASK_STATUS status)
    {
        reducer_status.put(reducer_id, status);
    }

    /**
     * Get all mapper's status
     * @return all mapper's status
     */
    public HashMap<String, TASK_STATUS> get_mapperStatus()
    {
        return mapper_status;
    }

    /**
     * Get all reducer's status
     * @return all reducer's status
     */
    public HashMap<String, TASK_STATUS> get_reducerStatus()
    {
        return reducer_status;
    }

    /**
     * Increment number of mapper's
     */
    public void inc_mapperct()
    {
        this.mapper_ct += 1;
    }

    /**
     * Increment number of reducer's
     */
    public void inc_reducerct()
    {
        this.reducer_ct += 1;
    }

    /**
     * Get number of mappers
     * @return number of mappers
     */
    public int get_mapperct()
    {
        return this.mapper_ct;
    }

    /**
     * Get number of reducers
     * @return number of mappers
     */
    public int get_reducerct()
    {
        return this.reducer_ct;
    }

    /**
     * Get job id
     * @return job id
     */
    public String get_jobId()
    {
        return this.job_id;
    }

    /**
     * Set mapper's class
     * @param class1 mapper's class
     * @param class_path where the class is stored
     */
    public void set_mapper(Class<? extends Mapper> class1, String class_path)
    {
        this.mapper = class1;
        this.mapper_clspath = class_path;
    }

    /**
     * Get mapper's class
     * @return mapper's class
     */
    public Class<? extends Mapper> get_mapper_cls()
    {
        return this.mapper;
    }

    /**
     * Get mapper's class path
     * @return mapper's class path
     */
    public String get_mapper_clspath()
    {
        return mapper_clspath;
    }

    /**
     * Set reducer's class
     * @param class1 reducer's class
     * @param class_path where the class is stored
     */
    public void set_reducer(Class<? extends Reducer> class1, String class_path)
    {
        this.reducer = class1;
        this.reducer_clspath = class_path;
    }

    /**
     * Get reducer's class path
     * @return reducer's class path
     */
    public String get_reducer_clspath()
    {
        return reducer_clspath;
    }

    /**
     * Get reducer's class
     * @return reducer's class
     */
    public Class<? extends Reducer> get_reducer_cls()
    {
        return this.reducer;
    }

    /**
     * Set input path
     * @param path input path
     */
    public void set_fileInputPath(String path)
    {
        this.fileInputPath = path;
        String tmp[] = path.split("/");
        fname = tmp[tmp.length-1];
    }

    /**
     * Set output path
     * @param path output path
     */
    public void set_fileOutputPath(String path)
    {
        this.fileOutputPath = path;
    }

    /**
     * Get output path
     * @return output path
     */
    public String get_fileOutputPath()
    {
        return this.fileOutputPath;
    }

    /**
     * Get input filename
     * @return filename
     */
    public String get_fileName()
    {
        return this.fname;
    }

    /**
     * Submit the job
     */
    public void submit() throws RemoteException
    {
        jobTracker.schedule(this);
    }
}
