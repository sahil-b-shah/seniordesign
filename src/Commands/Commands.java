package Commands;

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
	 */
	public static boolean createTable(String cmd) {
		for(Node node: ClusterManager.getNodes()){
			try {
				//create same new table on each node (same command for each node)
				node.sendMessage(cmd);
				return node.updateSuccessful();
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
