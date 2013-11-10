package mr;

import java.util.Iterator;
import mr.io.Writable;
import mr.io.TextWritable;

public class ReducerImpl extends Reducer<TextWritable, TextWritable, TextWritable, TextWritable>
{
  private static final long serialVersionUID = 98l;

  /*public ReducerImpl(String dirname)
    { 
	  super(dirname); 
	  
    }*/

  public static void main(String[] args) throws Exception
  {
    //ReducerImpl reducer = new ReducerImpl(args[0]);
	ReducerImpl reducer = new ReducerImpl();
    reducer.init(args[0]);
    reducer.bootstrap();
    Record record = null;
    while ((record = reducer.getNext()) != null) {
      String key = (String) record.getKey().getVal();
      System.out.println(key);
      Iterable<Writable> values = (Iterable<Writable>) record.getValues();
      Iterator<Writable> itor = values.iterator();
      while (itor.hasNext()) {
        Writable w = (Writable) itor.next();
        System.out.print(" " + w.getVal());
      }
      System.out.println();
    }
  }
}
