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

import MessageConnection.Message;
import MessageConnection.NodeToNodeConnectionThread;

public class NodeManager {

	private static String nodeConfigFileLocation = "./src/node_config.json";
	private static ServerSocket socket;
	private static BlockingQueue<Message> queue;
	private static String masterIP;
	private static int masterPort;
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
	
	private static void initializeNodes() throws IOException, JSONException {
		// initialize the cluster manager node
		File f = new File(nodeConfigFileLocation);
		InputStream is = new FileInputStream(f);
		String contents = readContentsOfFile(is);
		JSONObject json = new JSONObject(contents);
		masterIP = json.get("master_ip").toString();
		masterPort = Integer.parseInt(json.get("master_port").toString());
		curNodeIP = json.getString("node_ip").toString();
		curNodePort = Integer.parseInt(json.getString("node_port").toString());
	}
	
	public static void main(String args[]){
		if (args.length < 1) {
			System.out.println("Usage: NodeManager <curNodeIndex> <numThreads>");
			return;
		}

		numThreads = Integer.parseInt(args[1]);
		
//		nodes = null;
		
		System.out.println("Initializing Node " + args[0] + "...");

		try {
			initializeNodes();
		} catch (IOException e) {
			System.out.println("IOException while initializing nodes. Exiting");
			e.printStackTrace();
			return;
		} catch (JSONException e) {
			System.out.println("JSONException while parsing config files. Exiting");
			return;
		}
		
		System.out.println("Done setup");
		queue = new LinkedBlockingQueue<Message>();
		
		//NodeSendStatusThread statusThread = new NodeSendStatusThread(masterIP, masterPort);
		daemonThread = new NodeDaemonThread(socket, queue);
		threadPool = new ArrayList<NodeToNodeConnectionThread>();
		
		for (int i = 0; i < numThreads; i++) {
			NodeToNodeConnectionThread t = new NodeToNodeConnectionThread(queue,
					masterIP, masterPort, curNodeIP, curNodePort);
			threadPool.add(t);
			((Thread) t).start();
		}
		
		//statusThread.start();
		daemonThread.start();
	}
}
