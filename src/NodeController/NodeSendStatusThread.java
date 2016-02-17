package NodeController;

import java.io.PrintWriter;
import java.net.Socket;

public class NodeSendStatusThread extends Thread{
	
	private String masterIP;
	private int masterPort;
	private int localPort;
	
	public NodeSendStatusThread(String masterIP, int masterPort, int localPort) {
		this.masterIP = masterIP;
		this.masterPort = masterPort;
		this.localPort = localPort;
	}
	
	@Override
	public void run() {
		while(true){		
			try {
				Socket socket = new Socket(masterIP, 8085);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				out.println("STATUS UPDATE,"+localPort);
				out.flush();
				socket.close();
				Thread.sleep(5000);
			}
			catch (Exception e) {
				System.out.println("Error occured in node when sending status. May be ClusterManager is not up yet. Will retry in 15 secs");
				try {
					Thread.sleep(5000);
				}
				catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

}
