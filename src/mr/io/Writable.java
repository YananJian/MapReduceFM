package mr.io;

public interface Writable extends java.io.Serializable{
	/**
	 * get value
	 * @return
	 */
    public Object getVal();
    /**
     * set value
     * @param val
     */
    public void setVal(Object val);
}
