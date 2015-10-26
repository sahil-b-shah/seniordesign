package NodeConnection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.util.concurrent.ArrayBlockingQueue;

import Manager.ClusterManager;
import Manager.Message;

public class NodeConnectionThread implements Runnable {

	private Socket socket;
	private boolean updateSuccessful;
	private ArrayBlockingQueue<Message> queue;
	
	public NodeConnectionThread(ArrayBlockingQueue<Message> queue){
		this.queue = queue;
	}

	private Socket getSocket(String address, int port) throws UnknownHostException, IOException{
		return new Socket(address, port);
	}
	
	public String sendMessage(String query, String type, String ip, int port){
		try {
			socket = getSocket(ip, port);
			PrintWriter pw = new PrintWriter(socket.getOutputStream());
			pw.println(query + "\r\n\r\n");
			pw.flush();
			
			BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String response = "";
			String r;
			while((r = br.readLine()) != null) {
				response += r + "\n";
			}
			br.close();

			socket.close();
			socket = null;
			if (response.toLowerCase().contains("success")) {
				System.out.println("Successfully sent and received command");
				updateSuccessful = true;
			}
			
//			pw.close();
			return response;
		} catch (IOException e) {
			System.out.println("IOException in sendMessage()");
			return "IOException";
		}
	}
	
	public void resetUpdate() {
		this.updateSuccessful = false;
	}
	
	@Override
	public void run() {
		while(true) {
			try {
				Message m = queue.take();
				resetUpdate();
				String result = sendMessage(m.getCommand(), m.getType(), m.getIp(), m.getPort());
				ClusterManager.incrementSuccessCount(result, m.getNodeNum(), updateSuccessful);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	
}
