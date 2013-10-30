package mr.common;

import mr.common.Constants.JOB_STATUS;
import mr.common.Constants.MSG_TP;


public class Msg implements java.io.Serializable{
	
	private MSG_TP msg_tp = null;
	private Object content = null;
	private String job_id = null; // MAC + Process_ID
	private JOB_STATUS job_stat = null;
	private String blk_fpath = null;
	
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
	
	
}
