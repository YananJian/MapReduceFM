package mr.io;

/**
 * Writable, an interface for all Writable classes
 * @author Yanan Jian
 * @author Erdong Li
 */
public interface Writable extends java.io.Serializable{
    /**
     * get value
     * @return value
     */
    public Object getVal();

    /**
     * set value
     * @param val value
     */
    public void setVal(Object val);
}
