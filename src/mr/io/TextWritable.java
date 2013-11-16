package mr.io;

public class TextWritable implements java.io.Serializable, Writable{
    private String val = null;

    /**
     * set string value
     */
    public String getVal() {
        return val;
    }

    /**
     * get string value
     */
    @Override
    public void setVal(Object val) {
        // TODO Auto-generated method stub
        this.val = (String) val;
    }
}
