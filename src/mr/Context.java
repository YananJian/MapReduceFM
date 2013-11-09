package mr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import mr.io.Writable;

public class Context {

	String job_id = null;
	String task_id = null; //mapper id or reducer id
	BufferedWriter bw = null;
	HashMap<String, String> keyId = new HashMap<String, String>();
	HashMap<String, Integer> idSize = new HashMap<String, Integer>();
	int reducer_ct = 0;
	String dir = "";
	
	public Context(String job_id, String task_id, int reducer_ct, String dir)
	{
		this.job_id = job_id;
		this.task_id = task_id;
		this.reducer_ct = reducer_ct;
		this.dir = dir;
	}
	
	/*public void set_JobID(String id)
	{
		this.job_id = id;
	}
	
	public void set_TaskID(String id)
	{
		this.task_id = id;
	}*/
	
	public HashMap<String, Integer> get_idSize()
	{
		return this.idSize;
	}
	
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
			
			int m_id = key.getVal().hashCode() % reducer_ct;
			File f = new File(dir);
			if (!f.exists())
				f.mkdirs();
			//String path = dir + job_id+"@"+task_id + "@" +String.valueOf(m_id);
			String path = dir + '/' + String.valueOf(m_id);
			Integer line_ct = idSize.get(String.valueOf(m_id));
			if (line_ct != null)
				line_ct ++;
			else
				line_ct = 1;
			idSize.put(String.valueOf(m_id), line_ct);	
			keyId.put((String) key.getVal(), String.valueOf(m_id));
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(path, true));			
			bw.write(key.getVal() + "\t" + value.getVal()+"\n");
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
