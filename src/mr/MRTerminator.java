package mr;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import mr.core.JobTracker;


public class MRTerminator {

    private String registryHost;
    private int registryPort;
    public MRTerminator(String registryHost, int registryPort)
    {
        this.registryHost = registryHost;
        this.registryPort = registryPort;
    }

      /**
       * Terminate all jobs and JobTracker, TaskTrackers through RMI call
       */
    public void terminate() throws NotBoundException, RemoteException
    {
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
        jobTracker.terminate();
    }

    public void kill() throws NotBoundException, RemoteException
    {
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
        jobTracker.kill();
    }

    public void kill(String jobID) throws NotBoundException, RemoteException
    {
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
        jobTracker.kill(jobID);
    }

    public static void main(String[] args) {
        String registryHost = args[0];
        int registryPort = Integer.parseInt(args[1]);
        String cmd = args[2];
        MRTerminator mrt = new MRTerminator(registryHost, registryPort);
        try {
            if (cmd.equals("t"))
                mrt.terminate();
            else if (cmd.equals("k"))
            {
                if (args.length > 3)
                    mrt.kill(args[3]);
                else
                    mrt.kill();
            }
          } catch (Exception e) {
            e.printStackTrace();
          }
    }
}
