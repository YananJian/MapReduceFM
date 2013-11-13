package testmr;

import java.util.Iterator;
import java.rmi.RemoteException;

import mr.Job;
import mr.Context;
import mr.Mapper;
import mr.Reducer;
import mr.io.Writable;
import mr.io.IntWritable;
import mr.io.TextWritable;

public class WikiMediaFilter
{
  public static class FilterMapper extends Mapper<TextWritable, TextWritable, TextWritable, IntWritable>
  {
    private static final String PATTERN_ENGLISH = "^en$";
    private static final String PATTERN_SPECIAL = "^Media|^Special|^Talk|^User|^Project|^File|^MediaWiki|^Template|^Help|^Category|^Portal|^Wikipedia";
    private static final String PATTERN_LOWERCASE = "^[a-z]";
    private static final String PATTERN_IMAGE = "\\.jpg$|\\.gif$|\\.png$|\\.JPG$|\\.GIF$|\\.PNG$|\\.txt$|\\.ico$";
    private static final String PATTERN_BOILERPLATE = "^404_error\\/$|^Main_Page$|^Hypertext_Transfer_Protocol$|^Search$";

    @Override
    public void map(TextWritable key, TextWritable value, Context context) {
      String[] tokens = value.getVal().split(" ");
      boolean isEnglish = tokens[0].matches(PATTERN_ENGLISH);
      boolean isSpecial = tokens[1].matches(PATTERN_SPECIAL);
      boolean isLowercase = tokens[1].matches(PATTERN_LOWERCASE);
      boolean isImage = tokens[1].matches(PATTERN_IMAGE);
      boolean isBoilerplate = tokens[1].matches(PATTERN_BOILERPLATE);
      if (isEnglish && !isSpecial && !isLowercase && !isImage && !isBoilerplate) {
        TextWritable v = new TextWritable();
        v.setVal(tokens[2]);
        context.write(key, v);
      }
    }
  }

  public static class FilterReducer extends Reducer<TextWritable, TextWritable, TextWritable, IntWritable>
  {
    @Override
    public void reduce(TextWritable key, Iterable<Writable> values, Context context)
    {
      int views = 0;
      Iterator<Writable> itor = values.iterator();
      while (itor.hasNext()) {
        views += Integer.parseInt(((TextWritable) itor.next()).getVal());
      }
      if (views > 100000) {
        TextWritable value = new TextWritable();
        value.setVal(views);
        context.write(key, value);
      }
    }
  }

  public static void main(String args[])
  {
    String input_path = args[0];
    String output_path = args[1];
    Job job = new Job();
    job.set_fileInputPath(input_path);
    job.set_fileOutputPath(output_path);
    job.set_mapper(FilterMapper.class);
    job.set_reducer(FilterReducer.class);
    try {
      job.submit();
    } catch (RemoteException e) {
      e.printStackTrace();
    }
  }
}
