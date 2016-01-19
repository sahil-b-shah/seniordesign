package NodeController;

import java.io.PrintWriter;
import java.net.Socket;

public class NodeSendStatusThread extends Thread{
	
	private String masterIP;
	private int masterPort;
	
	public NodeSendStatusThread(String masterIP, int masterPort) {
		this.masterIP = masterIP;
		this.masterPort = masterPort;
	}
	
	@Override
	public void run() {
		while(true){		
			try {
				Socket socket = new Socket(masterIP, masterPort);
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				//TODO: possibly send more information later
				out.println("STATUS UPDATE");
				out.flush();
				socket.close();
				Thread.sleep(15000);
			}
			catch (Exception e) {
				System.out.println("Error occured in node when sending status. May be ClusterManager is not up yet. Will retry in 15 secs");
				try {
					Thread.sleep(15000);
				}
				catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

}
