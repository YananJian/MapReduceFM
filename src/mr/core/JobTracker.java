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

import conf.Config;
import mr.Job;
import dfs.DataNode;
import dfs.NameNode;

public interface JobTracker extends Remote, java.io.Serializable{
	public void init() throws RemoteException;;
	public void schedule(Job job) throws RemoteException;;
	
}
