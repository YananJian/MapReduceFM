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
			System.out.println("reduced KEY:"+key.getVal());
			for (Writable val:values)
			{
				TextWritable _val = (TextWritable) val;
				System.out.println("reduced VAL:"+_val.getVal());
				sum += Integer.parseInt(_val.getVal());
			}
			System.out.println("-----result: "+key.getVal() + "\t"+String.valueOf(sum));
		}
	}
	public static void main(String args[])
	{
		//Job job = new Job();
		String input_path = "/Users/yanan/javapro/workspace/testfiles/t1";
		String output_path = "/Users/yanan/javapro/workspace/testfiles/t2";
		String tmp[] = input_path.split("/");
		Job job = new Job();
		job.set_fileInputPath(input_path);
		job.set_fileOutputPath(output_path);
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
