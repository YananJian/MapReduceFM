package mr;

/**
 * Mapper's class
 * @author Yanan Jian
 * @author Erdong Li
 */
public class Mapper <K1 , V1 , K2 , V2> implements java.io.Serializable{
    /**
     * map function for client to override
     * @param k1 key
     * @param v1 value
     * @param context context to be written on
     */
    public void map(K1 k1, V1 v1, Context context)
    {}
}
