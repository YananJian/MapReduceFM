package testmr;

import java.rmi.RemoteException;
import java.lang.Thread;

import mr.Context;
import mr.Job;
import mr.Mapper;
import mr.Reducer;
import mr.io.Writable;
import mr.io.IntWritable;
import mr.io.TextWritable;

public class WordCount
{
  public static class WordCountMapper extends Mapper<TextWritable, TextWritable, TextWritable, IntWritable>
  {

    @Override
    public void map(TextWritable key, TextWritable val, Context context) {
      TextWritable v = new TextWritable();
      v.setVal("1");
      try {
        Thread.sleep(5000);
      } catch (Exception e) {
        // igore
      }
      context.write(key, v);
    }
  }

  public static class WordCountReducer extends Reducer<TextWritable, TextWritable, TextWritable, IntWritable>
  {
    @Override
    public void reduce(TextWritable key, Iterable<Writable> values, Context context)
    {
      int sum = 0;
      IntWritable value = new IntWritable();
      for (Writable v : values) {
        TextWritable tw = (TextWritable) v;
        try {
          Thread.sleep(5000);
        } catch (Exception e) {
          // ignore
        }
        sum += Integer.parseInt(tw.getVal());
      }
      value.setVal(sum);
      context.write(key, value);
    }
  }

  public static void main(String args[])
  {
    String input_path = args[0];
    String output_path = args[1];
    Job job = new Job(args[2], Integer.parseInt(args[3]));
    job.set_fileInputPath(input_path);
    job.set_fileOutputPath(output_path);
    job.set_mapper(WordCountMapper.class, "testmr/WordCountMapper.class");
    job.set_reducer(WordCountReducer.class, "testmr/WordCountReducer.class");
    try {
      job.submit();
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }
}
