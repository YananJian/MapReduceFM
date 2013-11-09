package mr.common;

public class Constants {
	
	public enum MSG_TP {
		REQ_BLK, REL_BLK, SET_JOB_STATUS, 
		START_MAPPER, TERMINATE_MAPPER, 
		};
	
	public enum JOB_STATUS{
		STARTING, STARTED, TERMINATED, FINISHED
	}
	
	public enum TASK_STATUS{
		RUNNING, FINISHED, ERROR
	}
	
	public enum TASK_TP{MAPPER, REDUCER}
}
