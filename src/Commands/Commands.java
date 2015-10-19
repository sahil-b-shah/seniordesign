package Commands;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONException;

import Manager.ClusterManager;
import NodeConnection.NodeConnection;

public class Commands {
	
	private final static char REPLACEMENT_CHAR = '\uFFFC';

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
		
		final char[] rest = cmd.substring(cmd.indexOf("VALUES")).toCharArray();
		
		String[] values = getValues(rest);
		primaryKey = values[0];
		
		String hashedValue = DigestUtils.sha1Hex(primaryKey);
		int nodeNumber = pickNumberBucket(ClusterManager.getNodesSize(), hashedValue);
		
		return ClusterManager.sendMessageToNode(cmd, nodeNumber);
	}
	
	private static String[] getValues(char[] chars) {
		boolean inString = false;
	    int start = -1;
	    int end   = -1;
	    for (int i = 0; i<chars.length; i++)
	    {
	        final char c = chars[i];
	        if ( inString && c == ',')
	        {
	            chars[i] = REPLACEMENT_CHAR;
	        }
	        else if ( c=='\'')
	        {
	            inString = !inString;
	        }
	        else if (!inString && c == '(')
	        {
	            if (start != -1)
	                throw new AssertionError("Start brace found twice!");

	            start = i+1;
	        }
	        else if (!inString && c == ')')
	        {
	            end = i-1;
	            break;
	        }
	    }

	    if (start == -1 || end == -1)
	        throw new AssertionError("Start or end of the values part not found!");


	    // split the result for having the values
	    final String[] values = new String(chars, start, end-start+1).split("\\s*,\\s*");
	    /*if (values.length != columns.length)
	        throw new AssertionError("The number of values differs of the number of columns");*/
	    return values;
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
	private static int pickNumberBucket(int numNodes, String hashedValue) {
		String maxValue = "";
		for(int i = 0; i < 40; i++){
			maxValue += "f";
		}
		BigInteger hash = new BigInteger(hashedValue, 16);
		BigInteger bigMax = new BigInteger(maxValue, 16).add(BigInteger.ONE);
		
		BigInteger rangeSize = bigMax.divide(BigInteger.valueOf(numNodes));
		
		int bucket = hash.divide(rangeSize).intValue() + 1;
		return bucket;
	}

	
	
}
