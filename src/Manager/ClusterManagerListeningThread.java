package Manager;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class ClusterManagerListeningThread extends Thread {
	
	private ServerSocket serverSocket;
	private int port;
	public BlockingQueue<Socket> queue;
	
	public ClusterManagerListeningThread(int port, BlockingQueue<Socket> queue){
		this.port = port;
		this.queue = new LinkedBlockingQueue<Socket>();
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
