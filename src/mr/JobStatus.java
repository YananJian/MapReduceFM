package mr;

import java.util.Hashtable;

import mr.common.Constants;

public class JobStatus {

	//mapper_ID, progress
	Hashtable<String, Float> mapper_Progress = new Hashtable<String, Float>();
	Hashtable<String, Float> reducer_Progress = new Hashtable<String, Float>();
	Hashtable<String, Constants.JOB_STATUS> mapper_Status = new Hashtable
																<String, Constants.JOB_STATUS>();
    Hashtable<String, Constants.JOB_STATUS> reducer_Status = new Hashtable
																<String, Constants.JOB_STATUS>();															
	
	Constants.JOB_STATUS job_stat = null;				
	
	public void set_job_stat(Constants.JOB_STATUS stat)
	{
		this.job_stat = stat;
	}
	public Constants.JOB_STATUS get_job_stat()
	{
		return this.job_stat;
	}
	public void set_mapper_progress(String mapper_id, Float progress)
	{
		this.mapper_Progress.put(mapper_id, progress);
	}
	public Float get_mapper_progress(String mapper_id)
	{
		return this.mapper_Progress.get(mapper_id);
	}
	public void set_reducer_progress(String reducer_id, Float progress)
	{
		this.reducer_Progress.put(reducer_id, progress);
	}
	public Float get_reducer_progress(String reducer_id)
	{
		return this.reducer_Progress.get(reducer_id);
	}
	public void set_mapper_status(String mapper_id, Constants.JOB_STATUS status)
	{
		this.mapper_Status.put(mapper_id, status);
	}
	public void set_reducer_status(String reducer_id, Constants.JOB_STATUS status)
	{
		this.reducer_Status.put(reducer_id, status);
	}
	public Constants.JOB_STATUS get_mapper_status(String mapper_id)
	{
		return this.mapper_Status.get(mapper_id);
	}
	public Constants.JOB_STATUS get_reducer_status(String reducer_id)
	{
		return this.reducer_Status.get(reducer_id);
	}
}
