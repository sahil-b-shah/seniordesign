package Manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;

public class ClusterSendInitConfigThread extends Thread {
	private Map<Integer, String> nodeMap;
	private Map<String, String> nodeDBMap;
	private int index;
	
	public ClusterSendInitConfigThread(Map<Integer, String> nodeMap, Map<String, String> nodeDBMap, int i) {
		this.nodeMap = nodeMap;
		this.nodeDBMap = nodeDBMap;
		this.index = i;
	}
	
	private String getNodeString() {
		StringBuilder sb = new StringBuilder();
		
		for (Entry<Integer, String> e : nodeMap.entrySet()) {
			sb.append(e.getValue());
			sb.append(";");
		}
		
		String nodeString = sb.toString();
		return nodeString.substring(0, nodeString.length() - 1);
	}
	
	public void run() {
		String nodeString = getNodeString();
		String[] nodeAddr = nodeMap.get(index).split(":");
		
		try {
			Socket s = new Socket(nodeAddr[0], Integer.parseInt(nodeAddr[1]));
			PrintWriter out = new PrintWriter(s.getOutputStream(), true);
			
			out.write("STATUS\r\n");
			out.write("db_addr=" + nodeDBMap.get(nodeMap.get(index)) + "\r\n");
			out.write("nodes=" + nodeString + "\r\n");
			out.write("\r\n");
			
			out.flush();
			
			BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			String line;
			
			while ((line = br.readLine()) != "\r\n") {
				if (line.trim().equals("READY")) {
					try {
						ClusterManager cm = ClusterManager.getInstance();
						cm.incrementReadyCounter();
						break;
					}
					catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
			
			s.close();
			
		}
		catch (NumberFormatException e) {
			e.printStackTrace();
		}
		catch (UnknownHostException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}

}
