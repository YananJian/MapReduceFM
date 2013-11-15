package mr.io;

public class TextWritable implements java.io.Serializable, Writable{
    private String val = null;

    public String getVal() {
        return val;
    }

    @Override
    public void setVal(Object val) {
        // TODO Auto-generated method stub
        this.val = (String) val;
    }
}
