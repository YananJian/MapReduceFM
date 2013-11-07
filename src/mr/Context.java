package mr;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import mr.io.Writable;

public class Context {

	String job_id = null;
	String task_id = null; //mapper id or reducer id
	BufferedWriter bw = null;
	public Context(String job_id, String task_id)
	{
		this.job_id = job_id;
		this.task_id = task_id;
	}
	
	/*public void set_JobID(String id)
	{
		this.job_id = id;
	}
	
	public void set_TaskID(String id)
	{
		this.task_id = id;
	}*/
	
	public String get_JobID()
	{
		return this.job_id;
	}
	
	public String get_TaskID()
	{
		return this.task_id;
	}
	
	public void write(Writable key, Writable value)
	{
		try {
			String path = job_id+"@"+task_id;
			BufferedWriter bw = new BufferedWriter(new FileWriter(path, true));			
			bw.write(key.getVal() + "\t" + value.getVal()+"\n");
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
