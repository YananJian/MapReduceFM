package mr.io;

public class IntWritable implements java.io.Serializable, Writable{
    private Integer val = null;

    /**
     * get integer value
     */
    public Integer getVal() {
        return val;
    }

    /**
     * set integer value
     */
    @Override
    public void setVal(Object val) {
        this.val = (Integer)val;
    }
}
