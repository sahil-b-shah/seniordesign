package Manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import MessageConnection.MasterToNodeConnectionThread;


public class ClusterManager {
	
	private static ClusterManager managerInstance;
	private static String clusterConfigFileLocation = "./src/cluster_config.json";
	private String ip; // cluster manager's ip
	private int port; // cluster manager's port
	private ServerSocket clusterSocket;
	private ArrayBlockingQueue<Message> messageQueue;
	private Map<Integer, Thread> threadMap;
	private int nodeCount;
	private Map<Integer, String> nodeMap;
	private Map<String, String> nodeDBMap;
	private Map<String, Job> jobs;
	private SecureRandom random = new SecureRandom();
	private HashMap<String, Long> statusMap;
	private ArrayBlockingQueue<Socket> statusQueue;
	private ClusterManagerDaemonThread lisThread;
	private ClusterManagerCheckStatusThread csThread;
	private ClusterManagerRequestThread sThread;

	private ClusterManager(JSONObject json) throws JSONException, IOException {
		jobs = new HashMap<String, Job>();
		ip = json.get("ip").toString();
		port = Integer.parseInt(json.get("port").toString());
		clusterSocket = new ServerSocket(port);
		
		JSONArray ndes = new JSONArray(json.get("nodes").toString());
		messageQueue = new ArrayBlockingQueue<Message>(10);
		nodeCount = ndes.length();
		int threadSize = json.getInt("master_to_node_connection_thread_pool");
		
		nodeMap = new HashMap<Integer, String>();
		nodeDBMap = new HashMap<String, String>();
		for (int i = 0; i < ndes.length(); i++) {
			JSONObject nde = new JSONObject(ndes.get(i).toString());
			String ip = nde.get("ip").toString();
			int port = Integer.parseInt(nde.get("port").toString());
			String db = nde.get("db_addr").toString();
			nodeMap.put(i, ip + ":" + port);
			nodeDBMap.put(ip + ":" + port, db);
		}	
		
		threadMap = new HashMap<Integer, Thread>();
		for (int i = 0; i < threadSize; i++) {
			threadMap.put(i, new Thread(new MasterToNodeConnectionThread(messageQueue)));
			threadMap.get(i).start();
		}
		
		statusQueue = new ArrayBlockingQueue<Socket>(50);
		lisThread =  new ClusterManagerDaemonThread(port, statusQueue, nodeDBMap);
		csThread = new ClusterManagerCheckStatusThread(statusMap);
		sThread = new ClusterManagerRequestThread(statusQueue, statusMap, nodeDBMap);
	}
	
	public static ClusterManager getInstance() throws IOException, JSONException{
		if(managerInstance == null){
			
			//TODO: set password, server, etc
			File f = new File(clusterConfigFileLocation);
			InputStream is = new FileInputStream(f);
			String contents = readContentsOfFile(is);
			JSONObject json = new JSONObject(contents);
			
			managerInstance = new ClusterManager(json);
			
		}
		
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
	
	public String sendMessagesToAllNodes(String cmd, String type) {
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
	
	public String sendMessageToNode(String cmd, String type, int nodeNumber) {
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
	public synchronized void recordNodeResponse(String jobId, String message, int nodeNum, boolean updateSuccessful) {
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
	
	public synchronized String getJobResult(String jobId) {
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
	
	public int getNodesSize() {
		return nodeMap.size();
	}

	private String getNextJobId() {
		return new BigInteger(130, random).toString(32);
	}
}
