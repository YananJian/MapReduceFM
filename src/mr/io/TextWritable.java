package mr.io;

/**
 * Text writable
 * @author Yanan Jian
 * @author Erdong Li
 */
public class TextWritable implements java.io.Serializable, Writable{
    private String val = null;

    /**
     * get string value
     * @return string value
     */
    public String getVal() {
        return val;
    }

    /**
     * set string value
     * @param val string value
     */
    @Override
    public void setVal(Object val) {
        // TODO Auto-generated method stub
        this.val = (String) val;
    }
}
