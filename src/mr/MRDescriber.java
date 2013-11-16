package mr;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

import mr.core.JobTracker;

/**
 * Get the current status of MapReducerFM
 * @author Yanan Jian
 * @author Erdong Li
 */
public class MRDescriber
{
    private String registryHost;
    private int registryPort;

    /**
     * Constructor
     * @param registryHost registry's host
     * @param registryPort registry's port
     */
    public MRDescriber(String registryHost, int registryPort)
    {
        this.registryHost = registryHost;
        this.registryPort = registryPort;
    }

    /**
     * Get the current status of a specific job
     * @param jobID job's id
     * @return string that describe the job's status
     * @throws NotBoundException
     * @throws RemoteException
     */
    public String describe(String jobID) throws NotBoundException, RemoteException
    {
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
        return jobTracker.desc_job(jobID);
    }

    /**
     * Get the current status of all jobs
     * @return string that describe the jobs' status
     * @throws NotBoundException
     * @throws RemoteException
     */
    public String describe() throws NotBoundException, RemoteException
    {
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
        return jobTracker.desc_jobs();
    }

    /**
     * Main method of MRDescriber
     * @param args command-line arguments
     */
    public static void main(String[] args)
    {
        String registryHost = args[0];
        int registryPort = Integer.parseInt(args[1]);
        MRDescriber describer = new MRDescriber(registryHost, registryPort);
        try {
            if (args.length == 2)
                System.out.println(describer.describe());
            else
                System.out.println(describer.describe(args[2]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
