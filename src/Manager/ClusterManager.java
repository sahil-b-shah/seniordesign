package Manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import NodeConnection.NodeConnectionThread;


public class ClusterManager {
	
	private static ClusterManager managerInstance;
	private static String clusterConfigFileLocation = "./src/cluster_config.json";
	private static String nodeConfigFileLocation = "./src/node_config.json";
	private static String ip; // cluster manager's ip
	private static int port; // cluster manager's port
	private static ServerSocket clusterSocket;
	private static ArrayBlockingQueue<Message> messageQueue;
	private static Map<Integer, Thread> threadMap;
	private static int nodeCount;
	private static Map<Integer, String> nodeMap;
	private static Map<String, Job> jobs;
	private static SecureRandom random = new SecureRandom();

	
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
		
		File f = new File(nodeConfigFileLocation);
		InputStream is = new FileInputStream(f);
		String contents = readContentsOfFile(is);
		
		JSONObject json = new JSONObject(contents);
		JSONArray ndes = new JSONArray(json.get("nodes").toString());
		
		messageQueue = new ArrayBlockingQueue<Message>(10);
		nodeCount = ndes.length();
		int threadSize = json.getInt("thread_size");
		
		nodeMap = new HashMap<Integer, String>();
		for (int i = 0; i < ndes.length(); i++) {
			JSONObject nde = new JSONObject(ndes.get(i).toString());
			String ip = nde.get("ip").toString();
			int port = Integer.parseInt(nde.get("port").toString());
			nodeMap.put(i, ip + ":" + port);
			
		}	
		
		threadMap = new HashMap<Integer, Thread>();
		for (int i = 0; i < threadSize; i++) {
			threadMap.put(i, new Thread(new NodeConnectionThread(messageQueue)));
			threadMap.get(i).start();
		}
		
		jobs = new HashMap<String, Job>();
	}
	
	public static String sendMessagesToAllNodes(String cmd, String type) {
		String jobId = getNextJobId();
		Job j = new Job(jobId, nodeCount);
		jobs.put(jobId, j);
		
		for (int i = 0; i < nodeCount; i++) {
			String[] nodeAddr = nodeMap.get(i).split(":");
			Message m = new Message(cmd, type, nodeAddr[0], Integer.parseInt(nodeAddr[1]), i, jobId);
			try {
				messageQueue.put(m);
			} 
			catch (InterruptedException e1) {
				e1.printStackTrace();
				return null;
			}
			
		}

		return jobId;
	}
	
	public static String sendMessageToNode(String cmd, String type, int nodeNumber) {
		String[] nodeAddr = nodeMap.get(nodeNumber).split(":");
		String jobId = getNextJobId();
		Job j = new Job(jobId, 1);
		jobs.put(jobId, j);
		
		Message m = new Message(cmd, type, nodeAddr[0], Integer.parseInt(nodeAddr[1]), nodeNumber, jobId);
		try {
			messageQueue.put(m);
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
		
		return jobId;
	}
	
	/** TODO: figure out scheme to receive result sets/exception notifications from various threads (may be 
	 *  create a concept of a job and sending a message to every node or a set of nodes is a job
	 */
	public static synchronized void recordNodeResponse(String jobId, String message, int nodeNum, boolean updateSuccessful) {
		Job j = jobs.get(jobId);
		if (j == null) {
			System.out.println("Trying to record a node response for a null job");
			return;
		}
		
		if (updateSuccessful) {
			j.addToResultSet(message, nodeNum);
		}
		else {
			j.addFailureNode(nodeNum);
		}
		jobs.put(jobId, j);
	}
	
	public static synchronized String getJobResult(String jobId) {
		Job j = jobs.get(jobId);
		if (j == null) {
			return "Invalid Job ID";
		}
		
		if(j.jobFinished()) {
			jobs.remove(jobId);
			return j.getResultSet();
		}
		
		return null;
	}
	
	public static int getNodesSize() {
		return nodeMap.size();
	}

	private static String getNextJobId() {
		return new BigInteger(130, random).toString(32);
	}
}
