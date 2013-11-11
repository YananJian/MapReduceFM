package conf;

public class Config {

	public static String MASTER_IP = "0.0.0.0";
	public static int MASTER_PORT = 12345;
	public static String COMPUTE_A_IP = "0.0.0.0";
	public static int COMPUTE_A_PORT = 12346;
	public static int SPLIT_SIZE = 3; // read every 3 lines and split
	
	public static int REDUCER_NUM = 2;
}
