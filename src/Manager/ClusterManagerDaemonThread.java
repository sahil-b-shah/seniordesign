package Manager;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class ClusterManagerDaemonThread extends Thread {
	
	private ServerSocket serverSocket;
	private int port;
	public BlockingQueue<Socket> queue;
	private Map<String, String> nodeDBMap;
	
	public ClusterManagerDaemonThread(int port, BlockingQueue<Socket> queue, Map<String, String> nodeDBMap){
		this.port = port;
		this.queue = new LinkedBlockingQueue<Socket>();
		this.nodeDBMap = nodeDBMap;
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
		//TODO: implement this
	}
	

}
