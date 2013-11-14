package testmr;

import java.lang.Thread;
import java.rmi.RemoteException;

import mr.*;
import mr.io.*;



public class Test {
	public static class TestMapper extends Mapper<TextWritable, TextWritable, TextWritable, IntWritable>
	{

		@Override
		public void map(TextWritable key, TextWritable val, Context context) {
			TextWritable _val = new TextWritable();
			_val.setVal("1");
			context.write(key, _val);
		}		
	}
	public static class TestReducer extends Reducer<TextWritable, TextWritable, TextWritable, IntWritable>
	{
		@Override
		public void reduce(TextWritable key, Iterable<Writable> values, Context context)
		{
			int sum = 0;
			IntWritable res_sum = new IntWritable();
			for (Writable val:values)
			{
				TextWritable _val = (TextWritable) val;
				sum += Integer.parseInt(_val.getVal());
			}
			res_sum.setVal(sum);
			context.write(key, res_sum);
			
			System.out.println("-----result: "+key.getVal() + "\t"+String.valueOf(sum));
		}
	}
	
	public static void main(String args[])
	{
		String input_path = args[0];
		String output_path = args[1];
		Job job = new Job(args[2], Integer.parseInt(args[3]));
		job.set_fileInputPath(input_path);
		job.set_fileOutputPath(output_path);
		job.set_mapper(TestMapper.class, "testmr/Test$TestMapper.class");
		job.set_reducer(TestReducer.class, "testmr/Test$TestReducer.class");
    try {
		  job.submit();
    } catch (RemoteException e) {
      e.printStackTrace();
    }
	}
}
