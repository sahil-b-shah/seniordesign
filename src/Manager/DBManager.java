package Manager;

import java.util.ArrayList;

public class DBManager {
	
	private static DBManager managerInstance;
	private static ArrayList<DBNode> nodes;
	
	public static DBManager getInstance(){
		if(managerInstance == null){
			managerInstance = new DBManager();
			initNodes();
		}
		
		//TODO: set password, server, etc
		
		return managerInstance;
	}
	
	private static void initNodes(){
		nodes = new ArrayList<DBNode>();
		
		//TODO: read from config file and add nodes to arraylist
		
		//TODO: send message to each node with the ip addresses of other nodes
		
	}
	
	public static ArrayList<DBNode> getNodes(){
		if(managerInstance == null){
			managerInstance = new DBManager();
			initNodes();
		}
		return nodes;
	}
}
