package Manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import MessageConnection.MasterToNodeConnectionThread;
import Utilities.NodeStatus;


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
	private int readyCounter;
	private int numReplicas;
	private Map<String, NodeStatus> nodeStatus;
	private Map<String, List<String>> nodeReplicas;

	private ClusterManager(JSONObject json) throws JSONException, IOException {
		nodeStatus = new HashMap<String, NodeStatus>();
		numReplicas = json.getInt("number_replicas");
		nodeReplicas = new HashMap<String, List<String>>();
		statusMap = new HashMap<String, Long>();
		jobs = new HashMap<String, Job>();
		ip = json.get("ip").toString();
		port = Integer.parseInt(json.get("port").toString());
		System.out.print("Here");
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
			nodeStatus.put(ip+":"+port, NodeStatus.STOPPED);
			nodeDBMap.put(ip + ":" + port, db);
			statusMap.put(ip+":"+port, System.currentTimeMillis());
		}	

		for (int i = 0; i < ndes.length(); i++) {
			ArrayList<String> reps = new ArrayList<String>(numReplicas);
			for(int j = 0; j < numReplicas; j++){
				reps.add(nodeMap.get((i + j + 1) % numReplicas));
			}
			nodeReplicas.put(ip+":"+port, reps);
		}

		threadMap = new HashMap<Integer, Thread>();
		for (int i = 0; i < threadSize; i++) {
			threadMap.put(i, new Thread(new MasterToNodeConnectionThread(messageQueue)));
			threadMap.get(i).start();
		}

		statusQueue = new ArrayBlockingQueue<Socket>(50);
		lisThread =  new ClusterManagerDaemonThread(port, statusQueue, nodeDBMap, nodeMap);
		lisThread.start();
		csThread = new ClusterManagerCheckStatusThread(statusMap);
		csThread.start();
		sThread = new ClusterManagerRequestThread(statusQueue, statusMap, nodeDBMap);
		sThread.start();
	}

	public synchronized static ClusterManager getInstance() throws IOException, JSONException{
			System.out.println("Get instance");
			if(managerInstance == null){
				System.out.println("Creating ClusterManager");
				//TODO: set password, server, etc
				File f = new File(clusterConfigFileLocation);
				InputStream is = new FileInputStream(f);
				String contents = readContentsOfFile(is);
				JSONObject json = new JSONObject(contents);

				managerInstance = new ClusterManager(json);
				System.out.println(managerInstance.ip);
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

	/**
	 * Sends messages to appropriate nodes in one job
	 * @param messages
	 * @param type
	 * @return
	 */
	public String sendMessages(Map<String, Integer> messages, String type) {
		String jobId = getNextJobId();
		Job j = new Job(jobId, messages.size());
		jobs.put(jobId, j);

		for(Entry<String, Integer> message: messages.entrySet()){
			int i = message.getValue();
			if(i==-1)
				continue;
			String[] nodeAddr = nodeMap.get(i).split(":"); 
			Message m = new Message(message.getKey(), type, nodeAddr[0], Integer.parseInt(nodeAddr[1]), i, jobId);
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

	public boolean ready(){
		return readyCounter == nodeMap.size();
	}

	public void incrementReadyCounter() {
		readyCounter++;
	}

	public int getNumberReplicas(){
		return numReplicas;
	}

	public int getActiveNodeReplica(String address){
		List<String> temp = nodeReplicas.get(address);
		for(int i =0; i < temp.size(); i++){
			if(nodeStatus.get(temp.get(i)) == NodeStatus.ACTIVE)
				return getNodeNumber(temp.get(i));
		}
		return -1;
	}

	public void setNodeStatus(String address,NodeStatus status){
		nodeStatus.put(address, status);
	}

	public String getNodeAddress(int number){
		return nodeMap.get(number);
	}

	public int getNodeNumber(String address){
		for(Entry<Integer, String> entry: nodeMap.entrySet()){
			if(entry.getValue().equals(address) && getNodeStatus(entry.getValue()) == NodeStatus.ACTIVE)
				return entry.getKey();
		}
		return -1;
	}

	public NodeStatus getNodeStatus(String address){
		return nodeStatus.get(address);
	}

	public List<String> getReplicas(String address){
		return nodeReplicas.get(address);
	}
}
