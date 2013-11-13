package mr;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import javax.swing.text.html.HTMLDocument.Iterator;

import dfs.FileUploader;
import mr.io.Writable;

public class Context {

	String job_id = null;
	String task_id = null; //mapper id or reducer id
	BufferedWriter bw = null;
	HashMap<String, String> keyId = new HashMap<String, String>();
	HashMap<String, Integer> idSize = new HashMap<String, Integer>();
	int reducer_ct = 0;
	String dir = "";
	LinkedList<Record> contents = new LinkedList<Record>();
	
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
	
	public LinkedList<Record> get_Contents()
	{
		return contents;
	}
	
	protected void partition() throws IOException
	{
		java.util.Iterator<Record> iter = contents.iterator();
		File f = new File(dir);
		if (!f.exists())
			f.mkdirs();
			
		while(iter.hasNext())
		{
			Record r = iter.next();
			String key = (String) r.getKey().getVal();
			
			Iterable<Writable> values = r.getValues();
			int m_id = key.hashCode() % reducer_ct;
			
			String path = dir + task_id + '@' + String.valueOf(m_id);
			BufferedWriter bw = new BufferedWriter(new FileWriter(path, true));
			for(Writable val: values)
			{
				String s = key + "\t" + val.getVal()+"\n";
				bw.write(s);
				System.out.println("Writing to disk, path:"+path+"\tcontent:"+s);
			}
			bw.close();
		}
		
	}
	
	
	
	public void write(Writable key, Writable value)
	{
		int m_id = key.getVal().hashCode() % reducer_ct;
		
		//String path = dir + job_id+"@"+task_id + "@" +String.valueOf(m_id);
		String path = dir + '/' + task_id + '@' + String.valueOf(m_id);
		Integer line_ct = idSize.get(String.valueOf(m_id));		
		Record record = new Record(key, path);		
		record.addValue(value);
		
		contents.add(record);		
		
		Collections.sort(contents);
		
		if (line_ct != null)
			line_ct ++;
		else
			line_ct = 1;
		idSize.put(String.valueOf(m_id), line_ct);	
		keyId.put((String) key.getVal(), String.valueOf(m_id));
	}
	
}
