package mr.core;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mr.Job;
import mr.common.Msg;
import dfs.DataNode;
import dfs.NameNode;

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
	 * @param Msg msg that contains tasktracker's current status
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
	 * @param TaskTracker: the tasktracker being registered
	 * @throws RemoteException 
	 */
    public void register(String machineID, TaskTracker tt) throws RemoteException;

	/**
	 * Terminate the mapreduce framework
	 * @throws RemoteException 
	 */
    public void terminate() throws RemoteException;
}
