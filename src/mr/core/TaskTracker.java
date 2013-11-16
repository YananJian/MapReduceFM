package mr.core;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import mr.Mapper;
import mr.Reducer;

public interface TaskTracker extends Remote, java.io.Serializable {
	/**
     * set number of reducers
     */
	public void set_reducer_ct(int ct) throws RemoteException;
    /**
     * start mapper task
     * @param job_id
     * @param mapper_id
     * @param block_id
     * @param read_from_machine machine to read block from 
     * @param mapper mapper Class
     * @param clspath path of mapper Class
     */
    public void start_map(String job_id, String mapper_id, String block_id, String read_from_machine, Class<? extends Mapper> class1, String clspath) throws RemoteException;
    /**
     * start reducer task
     * @param job_id
     * @param reducer_id
     * @param write_path path for writing outputs
     * @param reducer reducer Class
     * @param clspath path of reducer Class
     */
    public void start_reducer(String job_id, String reducer_id, String write_path, Class<? extends Reducer> reducer, String clspath) throws RemoteException;
    /**
     * write content to file
     * @param path path of file
     * @param content content to be write
     */
    public void writestr(String path, String content) throws RemoteException;
    /**
     * read content from file
     * @param path Path of file
     * @param name Filename
     */
    public String readstr(String path, String name) throws RemoteException;
    /**
     * read all partitioned filenames from the specified directory based on the hashedID
     * @param path DIR to read from
     * @param hashID partitions are hashed based on reducer numbers, 
     * this ID determines what partitions should be processed by a specific task
     */
    public List<String> readDIR(String path, String hashID) throws RemoteException;
    /**
     * terminate a task
     * @param taskID ID of the task
     */
    public void terminate(String taskID) throws RemoteException;
    /**
     * for JobTracker to call, thus to ensure the TaskTracker is still alive 
     */
    public void heartbeat() throws RemoteException;
    /**
     * terminate this TaskTracker
     */
    public void terminate_self() throws RemoteException;
}
