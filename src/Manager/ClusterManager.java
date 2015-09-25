package Manager;

import java.util.ArrayList;

import Node.Node;

public class ClusterManager {
	
	private static ClusterManager managerInstance;
	private static ArrayList<Node> nodes;
	
	public static ClusterManager getInstance(){
		if(managerInstance == null){
			managerInstance = new ClusterManager();
			initNodes();
		}
		
		//TODO: set password, server, etc
		
		return managerInstance;
	}
	
	private static void initNodes(){
		nodes = new ArrayList<Node>();
		
		//TODO: read from config file and add nodes to arraylist
		
		//TODO: send message to each node with the ip addresses of other nodes
		
	}
	
	public static ArrayList<Node> getNodes(){
		if(managerInstance == null){
			managerInstance = new ClusterManager();
			initNodes();
		}
		return nodes;
	}
}
