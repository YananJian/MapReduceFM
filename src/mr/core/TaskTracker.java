package mr.core;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import mr.Mapper;
import mr.Reducer;

public interface TaskTracker extends Remote, java.io.Serializable {
	public void set_reducer_ct(int ct) throws RemoteException;
	public void start_map(String job_id, String mapper_id, String block_id, String read_from_machine, Class<? extends Mapper> class1) throws RemoteException;
	public void start_reducer(String job_id, String reducer_id, String write_path, Class<? extends Reducer> reducer) throws RemoteException;
	public void writestr(String path, String content) throws RemoteException;
	public String readstr(String path, String name) throws RemoteException;
	public List<String> read_dir(String path, String hashID) throws RemoteException;
	public void terminate(String taskID) throws RemoteException;
}
