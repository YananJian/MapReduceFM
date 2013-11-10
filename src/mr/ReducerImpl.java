package mr;

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
      String value = (String) record.getValue().getVal();
      System.out.println(key + " " + value);
    }
  }
}
