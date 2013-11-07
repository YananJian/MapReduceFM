package mr.core;

import java.rmi.Remote;
import java.rmi.RemoteException;

import mr.Mapper;

public interface TaskTracker extends Remote, java.io.Serializable {

	public void start_map(String job_id, String mapper_id, String block_id, Class<? extends Mapper> class1) throws RemoteException;
	

}
