package Commands;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONException;

import Manager.ClusterManager;
import NodeConenction.NodeConnection;

public class Commands {

	public static boolean join(){
		//TODO: Implement join
		return false;
	}	

	/**
	 * 
	 * @param cmd
	 * @return
	 * @throws IOException
	 * @throws JSONException
	 */
	public static boolean insert(String cmd) throws IOException, JSONException {
		String primaryKey = "";
		
		//TODO: entire method needs to be written
		//Hash primary  key using SHA-1
		String hashedValue = DigestUtils.sha1Hex(primaryKey);
		int fileNumber = pickNumberBucket(ClusterManager.getNodesSize(), hashedValue);
		
		return false;
	}
	
	/**
	 * Creates a distributed db
	 * @param cmd: query from parser that starts with "CREATE DB"
	 * @return true if worked, else false
	 * @throws JSONException 
	 * @throws IOException 
	 */
	public static boolean createDB(String cmd) throws IOException, JSONException {
		return ClusterManager.sendMessagesToAllNodes(cmd);
	}

	/**
	 * Creates a distributed table
	 * @param cmd: query from parser that starts with "CREATE TABLE"
	 * @return true if worked, else false
	 * @throws JSONException 
	 * @throws IOException 
	 */
	public static boolean createTable(String cmd) throws IOException, JSONException {
		return ClusterManager.sendMessagesToAllNodes(cmd);
	}

	public static boolean delete(String cmd) {
		// TODO Auto-generated method stub
		return false;
	}

	public static boolean select(String cmd) {
		// TODO Auto-generated method stub
		return false;
	}
	
	/**
	 * Picks which bucket hash value goes in
	 * @param numWorkers - number of ranges to split
	 * @param hashedValue - value to place
	 * @return node that value hashes to
	 */
	private static int pickNumberBucket(int numWorkers, String hashedValue) {
		String maxValue = "";
		for(int i = 0; i < 40; i++){
			maxValue += "f";
		}
		BigInteger hash = new BigInteger(hashedValue, 16);
		BigInteger bigMax = new BigInteger(maxValue, 16).add(BigInteger.ONE);
		
		BigInteger rangeSize = bigMax.divide(BigInteger.valueOf(numWorkers));
		
		int bucket = hash.divide(rangeSize).intValue() + 1;
		return bucket;
	}

	
	
}
