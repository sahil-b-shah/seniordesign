package Manager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class ClusterManagerRequestThread extends Thread {
	
	private BlockingQueue<Socket> queue;
	private HashMap<String, Long> statusMap;
	private Map<String, String> nodeDBMap;
	private int readyCounter;
	
	public ClusterManagerRequestThread(BlockingQueue<Socket> queue, HashMap<String, Long> map, 
			Map<String, String> nodeDBMap){
		this.queue = queue;
		this.statusMap = map;
		this.nodeDBMap = nodeDBMap;
	}
	
	public void run() {
		while(true){
			try {
				Socket socket = queue.take();
				String ip = socket.getInetAddress().getHostAddress();
				int port = socket.getPort();
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String line = br.readLine();	
				if(line.equals("STATUS UPDATE")){
					statusMap.put(ip+":"+port, System.currentTimeMillis());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	

}
