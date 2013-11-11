package mr.common;

import mr.common.Constants.JOB_STATUS;
import mr.common.Constants.MSG_TP;
import mr.common.Constants.TASK_STATUS;
import mr.common.Constants.TASK_TP;


public class Msg implements java.io.Serializable{
	
	private MSG_TP msg_tp = null;
	private TASK_TP task_tp = null;
	private Object content = null;
	private String job_id = null; 
	private String task_id = null;
	private JOB_STATUS job_stat = null;
	private TASK_STATUS task_stat = null;
	private String blk_fpath = null;
	private String machine_id = null;
	int aval_procs = -1;
	//private HashMap<String, Integer> 
	
	public MSG_TP getMsg_tp() {
		return msg_tp;
	}

	public void setMsg_tp(MSG_TP msg_tp) {
		this.msg_tp = msg_tp;
	}

	public Object getContent() {
		return content;
	}

	public void setContent(Object content) {
		this.content = content;
	}

	public String getJob_id() {
		return job_id;
	}

	public void setJob_id(String job_id) {
		this.job_id = job_id;
	}

	public JOB_STATUS getJob_stat() {
		return job_stat;
	}

	public void setJob_stat(JOB_STATUS job_stat) {
		this.job_stat = job_stat;
	}

	public String getBlk_fpath() {
		return blk_fpath;
	}

	public void setBlk_fpath(String blk_fpath) {
		this.blk_fpath = blk_fpath;
	}

	public String getTask_id() {
		return task_id;
	}

	public void setTask_id(String task_id) {
		this.task_id = task_id;
	}

	public TASK_TP getTask_tp() {
		return task_tp;
	}

	public void setTask_tp(TASK_TP task_tp) {
		this.task_tp = task_tp;
	}

	public TASK_STATUS getTask_stat() {
		return task_stat;
	}

	public void setTask_stat(TASK_STATUS task_stat) {
		this.task_stat = task_stat;
	}

	public String getMachine_id() {
		return machine_id;
	}

	public void setMachine_id(String machine_id) {
		this.machine_id = machine_id;
	}
	
	public void set_aval_procs(int num)
	{
		this.aval_procs = num;
	}
	
	public int get_aval_procs()
	{
		return this.aval_procs;
	}
}
