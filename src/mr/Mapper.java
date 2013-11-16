package mr;

public class Mapper <K1 , V1 , K2 , V2> implements java.io.Serializable{
    /**
     * map function for client to override
     * @param k1
     * @param v1
     * @param context
     */
	public void map(K1 k1, V1 v1, Context context)
    {}
}
