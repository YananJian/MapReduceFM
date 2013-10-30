package mr;

public interface Mapper <K1, V1, K2, V2> extends java.io.Serializable{
	
	public void map(K1 key, V1 val);

	
}
