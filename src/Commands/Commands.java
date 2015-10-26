package Commands;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import Manager.ClusterManager;
import NodeConnection.NodeConnection;

public class Commands {

	private final static char REPLACEMENT_CHAR = '\uFFFC';
	private static String tablesSettingsFileLocation = "./src/tables_settings.json";

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

		String tableName = parseTableName(cmd, new String("INSERT INTO"));
		
		final char[] rest = cmd.substring(cmd.indexOf("VALUES")).toCharArray();

		String[] values = getValues(rest);
		//primaryKey = values[0];
		primaryKey = getConcatenatedPKsFromFile(tableName, values);

		String hashedValue = DigestUtils.sha1Hex(primaryKey);
		int nodeNumber = pickNumberBucket(ClusterManager.getNodesSize(), hashedValue);

		return ClusterManager.sendMessageToNode(cmd, "UPDATE", nodeNumber);
	}
	
	private static String getConcatenatedPKsFromFile(String tableName, String[] values) throws IOException, JSONException {
		File f = new File(tablesSettingsFileLocation);
		InputStream is = new FileInputStream(f);
		String contents = readContentsOfFile(is);
		String concatenated = "";
		
		JSONObject json = new JSONObject(contents);
		JSONArray pks = new JSONArray(json.get(tableName).toString());

		for (int i = 0; i < pks.length(); i++) {
			int pk = pks.getInt(i);
			
			System.out.println("PK Index: " + pk + "Value: " + values[pk]);
			concatenated = concatenated + values[pk];
		}
		
		return concatenated;
		
	}
	
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
		
		return ClusterManager.sendMessagesToAllNodes(cmd, "UPDATE");
	}

	/**
	 * Creates a distributed table
	 * @param cmd: query from parser that starts with "CREATE TABLE"
	 * @return true if worked, else false
	 * @throws JSONException 
	 * @throws IOException 
	 */
	public static boolean createTable(String cmd) throws IOException, JSONException {
		
		JSONObject obj = new JSONObject();
		obj.put(parseTableName(cmd, new String("CREATE TABLE")), parsePKIndices(cmd));
		
		try (FileWriter file = new FileWriter(tablesSettingsFileLocation, true)) {
			file.write(obj.toString());
			System.out.println("Succesfully wrote to settings file");
		}
		
		return ClusterManager.sendMessagesToAllNodes(cmd, "UPDATE");
	}

	public static boolean delete(String cmd) {
		// TODO Auto-generated method stub
		return false;
	}
	
	private static String parseTableName(String cmd, String sqlPrefix) {
		
		String tableName = cmd.substring(sqlPrefix.length() + 1, cmd.indexOf('('));
		tableName = tableName.replaceAll("\\s+", "");
		System.out.println("Table name: " + tableName);
		return tableName;
	}
	
	private static List<Integer> parsePKIndices(String cmd) {
		String temp = cmd.substring(cmd.indexOf('(')+1, cmd.lastIndexOf(')'));
		
		String[] columnDefinitions = temp.split(",");
		
		List<Integer> primarykeyIndices = new LinkedList<Integer>();
		
		Map<String, Integer> columns = new HashMap<String, Integer>();
		
		for (int i = 0; i < columnDefinitions.length; i++) {
			String definitionLine = columnDefinitions[i];
			
			if ((i < (columnDefinitions.length - 1)) || (!definitionLine.startsWith("PRIMARY KEY"))) {
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
	public static boolean select(String cmd) {
		if(ClusterManager.sendMessagesToAllNodes(cmd, "QUERY")){
			try {
				for(NodeConnection node: ClusterManager.getNodes()){
					System.out.println(node.getResultString());
				}
				return true;
			} 
			catch (IOException | JSONException e) {
				e.printStackTrace();
			}
		}
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
