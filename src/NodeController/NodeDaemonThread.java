package NodeController;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

import NodeConnection.NodeMessage;

public class NodeDaemonThread extends Thread {

	private ServerSocket socket;
	private BlockingQueue<NodeMessage> queue;

	public NodeDaemonThread(ServerSocket s, DBInstance db,
			BlockingQueue<NodeMessage> q) {
		this.socket = s;
		this.queue = q;
	}
	
	@Override
	public void run() {
		while (true) {
			try {
				Socket s = socket.accept();
				NodeMessage n = new NodeMessage(s, true);
				this.queue.put(n);
			} 
			catch (IOException e) {
				System.out.println("Error receiving message from master. Exiting");
				break;
			} 
			catch (InterruptedException e1) {
				System.out.println("InterruptedException while trying to add NodeMessage to blocking Q");
				e1.printStackTrace();
			}
		}
	}
}
