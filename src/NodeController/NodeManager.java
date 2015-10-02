package NodeController;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import NodeConenction.NodeConnection;

public class NodeManager {

	private static DBInstance db;
	private static ArrayList<NodeConnection> nodes;
	private static String clusterConfigFileLocation = "./src/cluster_config.json";
	private static String nodeConfigFileLocation = "./src/node_config.json";
	private static ServerSocket socket;
	private static int nodeNum;
	
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
		String ip = json.get("ip").toString();
		int port = Integer.parseInt(json.get("port").toString());
//		nodes.add(new Node(ip, port));
		
		f = new File(nodeConfigFileLocation);
		is = new FileInputStream(f);
		contents = readContentsOfFile(is);
		
		json = new JSONObject(contents);
		JSONArray ndes = new JSONArray(json.get("nodes").toString());
		
		for (int i = 0; i < ndes.length(); i++) {
			JSONObject nde = new JSONObject(ndes.get(i).toString());
			ip = nde.get("ip").toString();
			port = Integer.parseInt(nde.get("port").toString());
			if (i != nodeNum) {
//				nodes.add(new Node(ip, port));
			}
			else {
				socket = new ServerSocket(port);
			}
		}
	}
	
	public static void main(String args[]){
		if (args.length < 1) {
			System.out.println("You need to specify what node this manager is running on");
			return;
		}
		nodeNum = Integer.parseInt(args[0]);
		
		db = null;
		nodes = null;
		
		System.out.println("Initializing Node " + args[0] + "...");

		try {
			initializeNodes(nodeNum);
		} catch (IOException e) {
			System.out.println("IOException while initializing nodes. Exiting");
			return;
		} catch (JSONException e) {
			System.out.println("JSONException while parsing config files. Exiting");
			return;
		}
		
		//TODO: needs to constantly listen to socket and send queries based on commands from master 
		while(true) {
			try {
				Socket s = socket.accept();
				BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
				String line;
				while((line = in.readLine()) != null) {
					System.out.println(line);
				}
				
				System.out.println("-------\n");
			} catch (IOException e) {
				System.out.println("Error receiving message from master. Exiting");
				break;
			}
		}
	}
}
