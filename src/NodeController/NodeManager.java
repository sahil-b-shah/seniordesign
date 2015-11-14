package NodeController;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import NodeConnection.NodeMessage;
import NodeConnection.NodeToNodeConnectionThread;

public class NodeManager {

	private static DBInstance db;
	private static String clusterConfigFileLocation = "./src/cluster_config.json";
	private static String nodeConfigFileLocation = "./src/node_config.json";
	private static ServerSocket socket;
	private static int nodeNum;
	private static String dbAddr;
	private static BlockingQueue<NodeMessage> queue;
	private static String masterIP;
	private static int masterPort;
	private static Map<String, Integer> nodeAddrs;
	private static String curNodeIP;
	private static int curNodePort;
	private static int numThreads;
	private static List<NodeToNodeConnectionThread> threadPool;
	private static NodeDaemonThread daemonThread;
	
	private static String readContentsOfFile(InputStream is) throws IOException {
		StringBuilder sb = new StringBuilder();
		int ch = -1;
		while((ch = is.read()) != -1) {
			sb.append((char) ch);
		}
		
		return sb.toString();
	}
	
	private static void initializeNodes(int nodeNum) throws IOException, JSONException {
		// initialize the cluster manager node
		File f = new File(clusterConfigFileLocation);
		InputStream is = new FileInputStream(f);
		String contents = readContentsOfFile(is);
		JSONObject json = new JSONObject(contents);
		masterIP = json.get("ip").toString();
		masterPort = Integer.parseInt(json.get("port").toString());
		
		f = new File(nodeConfigFileLocation);
		is = new FileInputStream(f);
		contents = readContentsOfFile(is);
		
		json = new JSONObject(contents);
		JSONArray ndes = new JSONArray(json.get("nodes").toString());
		nodeAddrs = new HashMap<String, Integer>();
		
		for (int i = 0; i < ndes.length(); i++) {
			JSONObject nde = new JSONObject(ndes.get(i).toString());
			String ip = nde.get("ip").toString();
			int port = Integer.parseInt(nde.get("port").toString());
			if (i != nodeNum) {
				nodeAddrs.put(ip, port);
			}
			else {
				curNodeIP = ip;
				curNodePort = port;
				socket = new ServerSocket(port);
				dbAddr = nde.getString("db_addr").toString();
			}
		}
	}
	
	public static void main(String args[]){
		if (args.length < 1) {
			System.out.println("Usage: NodeManager <curNodeIndex> <numThreads>");
			return;
		}
		nodeNum = Integer.parseInt(args[0]);
		numThreads = Integer.parseInt(args[1]);
		
		db = null;
//		nodes = null;
		
		System.out.println("Initializing Node " + args[0] + "...");

		try {
			initializeNodes(nodeNum);
		} catch (IOException e) {
			System.out.println("IOException while initializing nodes. Exiting");
			e.printStackTrace();
			return;
		} catch (JSONException e) {
			System.out.println("JSONException while parsing config files. Exiting");
			return;
		}
		
		try {
			System.out.println("DBAddr: " + dbAddr);
			db = new DBInstance(dbAddr, 3306, 1);
		} catch (ClassNotFoundException e1) {
			System.out.println("Make sure that you have correctly imported the JDBC libs");
			return;
		} catch (SQLException e1) {
			e1.printStackTrace();
			System.out.println("SQL exception while setting up connection to db");
			return;
		}
		
		System.out.println("Done setup");
		queue = new LinkedBlockingQueue<NodeMessage>();
		
		daemonThread = new NodeDaemonThread(socket, db, queue);
		threadPool = new ArrayList<NodeToNodeConnectionThread>();
		
		for (int i = 0; i < numThreads; i++) {
			NodeToNodeConnectionThread t = new NodeToNodeConnectionThread(db, queue,
					masterIP, masterPort, nodeAddrs, curNodeIP, curNodePort);
			threadPool.add(t);
			((Thread) t).start();
		}
		
		daemonThread.start();
	}
}
