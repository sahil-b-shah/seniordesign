package Commands;

import java.io.IOException;

import org.json.JSONException;

import Manager.ClusterManager;
import Node.Node;

public class Commands {

	public static boolean join(){
		//TODO: Implement join
		return false;
	}	

	public static boolean insert(String cmd) {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * Creates a distributed table
	 * @param cmd: query from parser that starts with "CREATE TABLE"
	 * @return true if worked, else false
	 * @throws JSONException 
	 * @throws IOException 
	 */
	public static boolean createTable(String cmd) throws IOException, JSONException {
		for(Node node: ClusterManager.getNodes()){
			try {
				//create same new table on each node (same command for each node)
				if (!(node.sendMessage(cmd) || node.updateSuccessful())) {
					return false;
				}
			} catch (Exception e) {
				e.printStackTrace();
				return false;   //error in connection
			}
		}
		return true;   //no SQL exception anywhere
	}

	public static boolean delete(String cmd) {
		// TODO Auto-generated method stub
		return false;
	}

	public static boolean select(String cmd) {
		// TODO Auto-generated method stub
		return false;
	}
}
