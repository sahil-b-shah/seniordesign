package Manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import NodeConnection.NodeConnection;


public class ClusterManager {
	
	private static ClusterManager managerInstance;
	private static ArrayList<NodeConnection> nodes;
	private static String clusterConfigFileLocation = "./src/cluster_config.json";
	private static String nodeConfigFileLocation = "./src/node_config.json";
	private static String ip; // cluster manager's ip
	private static int port; // cluster manager's port
	private static ServerSocket clusterSocket;
	
	public static ClusterManager getInstance() throws IOException, JSONException{
		if(managerInstance == null){
			managerInstance = new ClusterManager();
			initNodes();
		}
		
		//TODO: set password, server, etc
		File f = new File(clusterConfigFileLocation);
		InputStream is = new FileInputStream(f);
		String contents = readContentsOfFile(is);
		JSONObject json = new JSONObject(contents);
		ip = json.get("ip").toString();
		port = Integer.parseInt(json.get("port").toString());
		clusterSocket = new ServerSocket(port);
		
		return managerInstance;
	}
	
	private static String readContentsOfFile(InputStream is) throws IOException {
		StringBuilder sb = new StringBuilder();
		int ch = -1;
		while((ch = is.read()) != -1) {
			sb.append((char) ch);
		}
		
		return sb.toString();
	}
	
	public static void initNodes() throws IOException, JSONException{
		if(managerInstance == null){
			managerInstance = new ClusterManager();
		}
		
		nodes = new ArrayList<NodeConnection>();

		//TODO: read from config file and add nodes to arraylist
		File f = new File(nodeConfigFileLocation);
		InputStream is = new FileInputStream(f);
		String contents = readContentsOfFile(is);
		
		JSONObject json = new JSONObject(contents);
		JSONArray ndes = new JSONArray(json.get("nodes").toString());
		
		for (int i = 0; i < ndes.length(); i++) {
			JSONObject nde = new JSONObject(ndes.get(i).toString());
			String ip = nde.get("ip").toString();
			int port = Integer.parseInt(nde.get("port").toString());
			System.out.println(ip + " " + port);
			nodes.add(new NodeConnection(ip, port));
		}
		
		//TODO: send message to each node with the ip addresses of other nodes
		
	}
	
	public static boolean sendMessagesToAllNodes(String cmd) {
		for(NodeConnection node: nodes){
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
		return true;
	}
	
	public static int getNodesSize() {
		return nodes.size();
	}
	
//	public static ArrayList<NodeConnection> getNodes() throws IOException, JSONException{
//		if(managerInstance == null){
//			managerInstance = new ClusterManager();
//			initNodes();
//		}
//		return nodes;
//	}
	
}
