package mr.core;

import java.rmi.Remote;
import java.rmi.RemoteException;

import mr.Job;
import mr.common.Msg;

/**
 * Remote Interface for JobTracker
 * @author Yanan Jian
 * @author Erdong Li
 */
public interface JobTracker extends Remote, java.io.Serializable{
  /**
   * Init JobTracker, create mrRegistry
   * @param registryHost mapreducefm's Registry host address
   * @param mrPort mapreducefm's registry port number
   * @param dfsPort dfs's port number
   * @param selfPort mapreducefm's port number
   * @param reducer_ct reducer numbers customized by client
   * @throws RemoteException
   */
    public void init(String registryHost, String mrPort, String dfsPort, String selfPort, String reducer_ct) throws RemoteException;

  /**
   * Schedule mapper tasks
   * @param job job to be scheduled
   * @throws RemoteException
   */
    public void schedule(Job job) throws RemoteException;
    //public void schedule_reducer(Job job) throws RemoteException;

    /**
     * Heartbeat with tasktrackers
     * @param msg that contains tasktracker's current status
     */
    public void heartbeat(Msg msg) throws RemoteException;

    /**
     * Describe job status
     * @param jobID
     */
    public String desc_job(String jobID) throws RemoteException;

    /**
     * Describe all job status
     * @throws RemoteException 
     */
    public String desc_jobs() throws RemoteException;

    /**
     * Print simple message for debug
     * @throws RemoteException 
     */
    public void print() throws RemoteException;

    /**
     * For Tasktrackers to register themselves
     * @param machineID machine that tasktracker is running on 
     * @param tt the tasktracker being registered
     * @throws RemoteException 
     */
    public void register(String machineID, TaskTracker tt) throws RemoteException;

    /**
     * Terminate the mapreduce framework
     * @throws RemoteException 
     */
    public void terminate() throws RemoteException;
    
    /**
     * kill all jobs
     */
    public void kill() throws RemoteException;
    
    /**
     * kill a specific job based on jobID
     * @param jobID
     */
    public void kill(String jobID) throws RemoteException;
}
