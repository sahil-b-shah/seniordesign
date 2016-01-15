package NodeController;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

import MessageConnection.Message;

public class NodeDaemonThread extends Thread {

	private ServerSocket socket;
	private BlockingQueue<Message> queue;

	public NodeDaemonThread(ServerSocket s,
			BlockingQueue<Message> q) {
		this.socket = s;
		this.queue = q;
	}
	
	@Override
	public void run() {
		while (true) {
			try {
				Socket s = socket.accept();
				Message n = new Message(s, true);
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
