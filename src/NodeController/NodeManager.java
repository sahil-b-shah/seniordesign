package NodeController;

import java.util.ArrayList;

import Node.Node;

public class NodeManager {

	private static DBInstance db;
	private static ArrayList<Node> nodes;
	
	public static void main(String args[]){
		System.out.println("Initializing Node Connection. Waiting for messges.");
		db = null;
		nodes = null;
		//TODO: needs to constantly listen to socket and send queries based on commands from master 
		
	}
}
