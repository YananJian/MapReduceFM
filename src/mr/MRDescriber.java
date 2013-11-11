package mr;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

import mr.core.JobTracker;

public class MRDescriber
{
    private String registryHost;
    private int registryPort;

    public MRDescriber(String registryHost, int registryPort)
    {
        this.registryHost = registryHost;
        this.registryPort = registryPort;
    }

    public String describe(String jobID) throws NotBoundException, RemoteException
    {
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
        return jobTracker.desc_job(jobID);
    }

    public String describe() throws NotBoundException, RemoteException
    {
        Registry registry = LocateRegistry.getRegistry(registryHost, registryPort);
        JobTracker jobTracker = (JobTracker) registry.lookup("JobTracker");
        return jobTracker.desc_jobs();
    }

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
