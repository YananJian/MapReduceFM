package testmr;
import java.rmi.RemoteException;

import mr.*;
import mr.io.*;



public class Test {
	public static class TestMapper extends Mapper<TextWritable, TextWritable, TextWritable, IntWritable>
	{

		@Override
		public void map(TextWritable key, TextWritable val, Context context) {
			// TODO Auto-generated method stub
			System.out.println("Mapping, Key:"+key.getVal()+"\tValue:"+val.getVal());
			context.write(key, val);
		}		
	}
	public static class TestReducer extends Reducer<TextWritable, TextWritable, TextWritable, IntWritable>
	{
		public void reduce(TextWritable key, Iterable<IntWritable> values, Context context)
		{
			int sum = 0;
			for (IntWritable val:values)
				sum += val.getVal();
			System.out.println(key + String.valueOf(sum));
		}
	}
	public static void main(String args[])
	{
		//Job job = new Job();
		String path = "/Users/yanan/javapro/workspace/testfiles/t1";
		String tmp[] = path.split("/");
		Job job = new Job();
		job.set_fileInputPath(path);
		job.set_fileOutputPath("s3://test");
		job.set_mapper(TestMapper.class);
		job.set_reducer(TestReducer.class);
		try {
			job.submit();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
