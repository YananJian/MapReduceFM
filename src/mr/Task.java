package mr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.*;

import mr.io.TextWritable;

public class Task implements Callable {
	Class<? extends Mapper> mapper = null;
	String job_id = null;
	String task_id = null;
	String read_dir = null;
	String block_id = null;
	String machine_id = null;
	int reducer_ct = 0;

	public Task(String job_id, String task_id, String block_id, int reducer_ct) {
		this.job_id = job_id;
		this.task_id = task_id;
		this.block_id = block_id;
		this.reducer_ct = reducer_ct;
	}

	public void set_mapper_cls(Class<? extends Mapper> mapper) {
		this.mapper = mapper;
	}

	public void set_read_dir(String dir) {
		this.read_dir = dir;
	}

	public void set_machineID(String m_id)
	{
		this.machine_id = m_id;
	}

	@Override
	public Object call() throws Exception {
		// TODO Auto-generated method stub
		try {
			System.out.println("------------Starting Mapper task in TaskTracker");

			Mapper<Object, Object, Object, Object> mapper_cls = mapper.newInstance();

			String output_tmpdir = "tmp/"+job_id+'/'+machine_id+'/';
			Context context = new Context(job_id, task_id, reducer_ct, output_tmpdir);

			System.out.println("Executing task, job id:" + job_id
					+ ", mapper_id:" + task_id);

			/* HARD CODING TEXTWRITABLE AS TYPE.... */
			TextWritable k1 = new TextWritable();
			TextWritable v1 = new TextWritable();
			/* read from block */
			BufferedReader br = new BufferedReader(new FileReader(read_dir
					+ "/" + block_id));
			String line = "";
			
			while ((line = br.readLine()) != null) {
				String k1_val = line;
				String v1_val = line;
				k1.setVal(k1_val);
				v1.setVal(v1_val);
				/* k1_val hashCode may conflict */

				mapper_cls.map(k1, v1, context);
			}
			return context.get_idSize();

		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
