package Manager;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class ClusterManagerDaemonThread extends Thread {
	
	private ServerSocket serverSocket;
	private int port;
	public BlockingQueue<Socket> queue;
	private Map<String, String> nodeDBMap;
	private Map<Integer, String> nodeMap;
	
	public ClusterManagerDaemonThread(int port, BlockingQueue<Socket> queue, Map<String, String> nodeDBMap,
			Map<Integer, String> nodeMap){
		this.port = port;
		this.queue = new LinkedBlockingQueue<Socket>();
		this.nodeDBMap = nodeDBMap;
		this.nodeMap = nodeMap;
	}
	
	public void run() {
		try {
			
			//send nodes information about nodes
			sendNodeUpdate();
			  
			serverSocket = new ServerSocket(port);
			Socket socket = null;
			while(true){
				socket = serverSocket.accept();
				queue.put(socket);
			}
		} 
		catch(Exception e){
			
		}
	}
	
	public void sendNodeUpdate(){
		List<Thread> threads = new ArrayList<Thread>();
		
		for (int i = 0; i < nodeMap.size(); i++) {
			threads.add(new ClusterSendInitConfigThread(nodeMap, nodeDBMap, i));
		}
		
		while (! threads.isEmpty()) {
			Thread t = threads.remove(0);
			t.start();
		}
	}
	

}
