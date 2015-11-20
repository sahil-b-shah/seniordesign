package Manager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

public class ClusterManagerStatusThread extends Thread {
	
	private BlockingQueue<Socket> queue;
	private HashMap<String, Long> statusMap;
	
	public ClusterManagerStatusThread(BlockingQueue<Socket> queue,HashMap<String, Long> map){
		this.queue = queue;
		this.statusMap = map;
	}
	
	public void run() {
		while(true){
			try {
				Socket socket = queue.take();
				String ip = socket.getInetAddress().getHostAddress();
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String line = br.readLine();	
				if(line.equals("STATUS UPDATE")){
					statusMap.put(ip, System.currentTimeMillis());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

}
