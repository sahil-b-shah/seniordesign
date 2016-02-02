package Commands;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import Manager.ClusterManager;
import Utilities.NodeStatus;

public class Commands {

	private final static char REPLACEMENT_CHAR = '\uFFFC';
	private static String tablesSettingsFileLocation = "./src/tables_settings.json";

	public static boolean inner_join(){
		//TODO: Implement join
		return false;
	}	

	/**
	 * Performs an insert of row
	 * @param cmd: insert command to run
	 * @return true if commands succeeds, false otherwise
	 * @throws IOException
	 * @throws JSONException
	 */
	public static boolean insert(String cmd) throws IOException, JSONException {
		HashMap<String, Integer> messages = new HashMap<String, Integer>();
		String primaryKey = "";

		String tableName = parseTableName(cmd, new String("INSERT INTO"));

		final char[] rest = cmd.substring(cmd.indexOf("VALUES")).toCharArray();

		String[] values = getValues(rest);
		//primaryKey = values[0];
		primaryKey = getConcatenatedPKsFromFile(tableName, values);
		System.out.println("Concatenaed PK: " + primaryKey);

		//Get actual insert node and add message
		String hashedValue = DigestUtils.sha1Hex(primaryKey);
		int nodeNumber = pickNumberBucket(ClusterManager.getInstance().getNodesSize(), hashedValue);
		messages.put(cmd, nodeNumber);

		//Change table name to denote replica
		String nodeAddress = ClusterManager.getInstance().getNodeAddress(nodeNumber);
		String cmdReplica = replaceTableName(cmd, "INSERT INTO", nodeAddress);

		//Add messages for replicas
		for(int i = 0; i < ClusterManager.getInstance().getNumberReplicas(); i++){
			int repNum = ClusterManager.getInstance().getNodeNumber(ClusterManager.getInstance().getReplicas(nodeAddress).get(i));
			messages.put(cmdReplica, repNum);
		}

		String jobId = ClusterManager.getInstance().sendMessages(messages, "UPDATE");

		while (true) {
			String result = ClusterManager.getInstance().getJobResult(jobId);
			if (result == null) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else if (result.equals("Invalid Job ID")) {
				return false;
			}
			return true;
		}

	}

	/**
	 * Gets primary keys from local storage
	 * @param tableName: table to get keys from
	 * @param values
	 * @return 
	 * @throws IOException
	 * @throws JSONException
	 */

	private static String getConcatenatedPKsFromFile(String tableName, String[] values) throws IOException, JSONException {
		File f = new File(tablesSettingsFileLocation);
		InputStream is = new FileInputStream(f);
		String contents = readContentsOfFile(is);
		String concatenated = "";

		JSONObject json = new JSONObject(contents);
		System.out.println("JSON read in " + json.toString());
		JSONArray pks = new JSONArray(json.get(tableName).toString());

		for (int i = 0; i < pks.length(); i++) {
			int pk = pks.getInt(i);

			System.out.println("PK Index: " + pk + "Value: " + values[pk]);
			concatenated = concatenated + values[pk];
		}

		return concatenated;

	}

	/**
	 * Reads content of file
	 * @param is - inputstream to read from
	 * @return String containing file containing
	 * @throws IOException
	 */
	private static String readContentsOfFile(InputStream is) throws IOException {
		StringBuilder sb = new StringBuilder();
		int ch = -1;
		while((ch = is.read()) != -1) {
			sb.append((char) ch);
		}

		return sb.toString();
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
		String jobId = ClusterManager.getInstance().sendMessagesToAllNodes(cmd, "UPDATE");

		while (true) {
			String result = ClusterManager.getInstance().getJobResult(jobId);
			if (result == null) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else if (result.equals("Invalid Job ID")) {
				return false;
			}
			return true;
		}		
	}

	/**
	 * Creates a distributed table
	 * @param cmd: query from parser that starts with "CREATE TABLE"
	 * @return true if worked, else false
	 * @throws JSONException 
	 * @throws IOException 
	 */
	public static boolean createTable(String cmd) throws IOException, JSONException {
		HashMap<String, Integer> messages = new HashMap<String, Integer>();
		//JSONObject obj = new JSONObject();
		File f = new File(tablesSettingsFileLocation);
		String contents = "";
		try {
			InputStream is = new FileInputStream(f);
			contents = readContentsOfFile(is);
		} catch (FileNotFoundException e) {

		}
		JSONObject obj;
		try {
			obj = new JSONObject(contents);
		} catch (JSONException e) {
			obj = new JSONObject();
		}
		obj.put(parseTableName(cmd, new String("CREATE TABLE")), parsePKIndices(cmd));

		try (FileWriter file = new FileWriter(tablesSettingsFileLocation, false)) {
			file.write(obj.toString());
			System.out.println("Succesfully wrote to settings file");
		}

		// Send table to all replicas
		for (int i = 0; i < ClusterManager.getInstance().getNodesSize(); i++) {
			messages.put(cmd, i);
			String nodeAddress = ClusterManager.getInstance().getNodeAddress(i);
			String cmdReplica = replaceTableName(cmd, "CREATE TABLE", nodeAddress);
			for(int j = 0; j < ClusterManager.getInstance().getNumberReplicas(); j++){
				int repNum = ClusterManager.getInstance().getNodeNumber(ClusterManager.getInstance().getReplicas(nodeAddress).get(j));
				messages.put(cmdReplica, repNum);
			}
		}
		
		String jobId = ClusterManager.getInstance().sendMessages(messages, "UPDATE");

		while (true) {
			String result = ClusterManager.getInstance().getJobResult(jobId);
			if (result == null) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else if (result.equals("Invalid Job ID")) {
				return false;
			}
			return true;
		}
	}

	public static boolean delete(String cmd) {
		// TODO implement this
		return false;
	}

	private static String parseTableName(String cmd, String sqlPrefix) {
		String tableName = cmd.substring(sqlPrefix.length() + 1, cmd.indexOf(' ', sqlPrefix.length()+2));
		tableName = tableName.replaceAll("\\s+", "");
		System.out.println("Table name: " + tableName);
		return tableName;
	}

	public static String replaceTableName(String cmd, String sqlPrefix, String ip){
		String newCmd = "";
		String tableName = parseTableName(cmd, sqlPrefix);
		String newTableName =  tableName + "REPLICA"+ip;
		newCmd = cmd.substring(0, sqlPrefix.length() + 1) + newTableName + 
				cmd.substring(sqlPrefix.length() + 1 + tableName.length());
		return newCmd;

	}

	private static List<Integer> parsePKIndices(String cmd) {
		//String temp = cmd.substring(cmd.indexOf('(')+1, cmd.lastIndexOf(')'));
		int index;
		String temp;
		if ((index = cmd.indexOf("PRIMARY KEY")) != -1) {
			temp = cmd.substring(cmd.indexOf('(')+1, index);
		} else {
			temp = cmd.substring(cmd.indexOf('(')+1, cmd.lastIndexOf(')'));
		}

		System.out.println("Cleaned string " + temp);

		String[] columnDefinitions = temp.split(",");

		List<Integer> primarykeyIndices = new LinkedList<Integer>();

		Map<String, Integer> columns = new HashMap<String, Integer>();

		for (int i = 0; i < columnDefinitions.length; i++) {
			String definitionLine = columnDefinitions[i];

			System.out.println("Definition Line" + definitionLine);

			String columnName = definitionLine.substring(0, definitionLine.indexOf(' '));
			System.out.println("Column[" + i +"]: " + columnName);
			columns.put(columnName.trim(), i);

			/*if ((i < (columnDefinitions.length - 1)) || (!definitionLine.startsWith("PRIMARY KEY"))) {
				String columnName = definitionLine.substring(0, definitionLine.indexOf(' '));
				System.out.println("Column[" + i +"]: " + columnName);
				columns.put(columnName, i);
			} else {
				String pkColumnNamesString = definitionLine.
						substring(definitionLine.indexOf('(')+1, definitionLine.lastIndexOf(')'));
				String[] pkColumnNames = pkColumnNamesString.split(",");

				System.out.println("PK Names: " + pkColumnNames);

				for (String columnName : pkColumnNames) {
					primarykeyIndices.add(columns.get(columnName));
				}

			}*/

		}

		//Return index values if statement contains specified primary keys, other wise return
		//array with assumed 0, as PK
		if (index != -1) {
			String PKLine = cmd.substring(index, cmd.lastIndexOf(')'));
			String pkColumnNamesString = PKLine.
					substring(PKLine.indexOf('(')+1, PKLine.lastIndexOf(')'));
			System.out.println("pkColumns: "+pkColumnNamesString);
			String[] pkColumnNames = pkColumnNamesString.split(",");

			for (String s : pkColumnNames) {
				primarykeyIndices.add(columns.get(s.trim()));
			}

		}
		return primarykeyIndices;


	}

	/**
	 * Selects data from databases
	 * @param cmd: query from parser that starts with "SELECT"
	 * @return true if worked, else false
	 * @throws JSONException 
	 * @throws IOException 
	 */
	public static boolean select(String cmd) throws IOException, JSONException {
		HashMap<String, Integer> messages = new HashMap<String, Integer>();
	
		for (int i = 0; i < ClusterManager.getInstance().getNodesSize(); i++) {
			String address = ClusterManager.getInstance().getNodeAddress(i);
			int nodeNumber = i;
			if(ClusterManager.getInstance().getNodeStatus(address) != NodeStatus.ACTIVE){
				nodeNumber = ClusterManager.getInstance().getActiveNodeReplica(address);
				cmd = replaceTableName(cmd, "SELECT", ClusterManager.getInstance().getNodeAddress(nodeNumber));
			}
			messages.put(cmd, nodeNumber);
		}
		
		String jobId = ClusterManager.getInstance().sendMessages(messages, "QUERY");

		while (true) {
			String result = ClusterManager.getInstance().getJobResult(jobId);
			if (result == null) {
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else if (result.equals("Invalid Job ID")) {
				return false;
			}

			System.out.println(result);
			return true;
		}
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

		int bucket = hash.divide(rangeSize).intValue();
		return bucket;
	}



}
