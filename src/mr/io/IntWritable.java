package mr.io;

public class IntWritable implements java.io.Serializable, Writable{

	private Integer val = null;
	
	public Integer getVal() {
		return val;
	}
	
	@Override
	public void setVal(Object val) {
		// TODO Auto-generated method stub
		this.val = (Integer)val;
	}
}
