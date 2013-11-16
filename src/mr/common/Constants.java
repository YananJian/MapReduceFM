package mr.common;

/**
 * Constants MapReduceFM uses
 * @author Yanan Jian
 * @author Erdong Li
 */
public class Constants {
    public enum MSG_TP {
        REQ_BLK, REL_BLK, SET_JOB_STATUS, 
        START_MAPPER, TERMINATE_MAPPER, 
        HEARTBEAT
        };

    public enum JOB_STATUS{
        RUNNING, TERMINATED, FINISHED
    }

    public enum TASK_STATUS{
        RUNNING, FINISHED, ERROR, TERMINATED
    }

    public enum TASK_TP{
        MAPPER, REDUCER
    }
}
