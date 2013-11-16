package mr.io;

/**
 * Int writable
 * @author Yanan Jian
 * @author Erdong Li
 */
public class IntWritable implements java.io.Serializable, Writable{
    private Integer val = null;

    /**
     * get integer value
     * @return interger value
     */
    public Integer getVal() {
        return val;
    }

    /**
     * set integer value
     * @param val the value
     */
    @Override
    public void setVal(Object val) {
        this.val = (Integer)val;
    }
}
