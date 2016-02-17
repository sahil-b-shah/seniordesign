package MessageConnection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;

import org.json.JSONException;

import Manager.ClusterManager;
import Manager.Message;

public class MasterToNodeConnectionThread implements Runnable {

	private Socket socket;
	private boolean updateSuccessful;
	private BlockingQueue<Message> queue;
	
	public MasterToNodeConnectionThread(BlockingQueue<Message> queue){
		this.queue = queue;
	}

	private Socket getSocket(String address, int port) throws UnknownHostException, IOException{
		return new Socket(address, port);
	}
	
	private String sendQuery(String query, String type, String ip, int port){
		try {
			socket = getSocket(ip, port);
			PrintWriter pw = new PrintWriter(socket.getOutputStream());
			pw.println(type);
			pw.println(query + "\r\n");
			pw.flush();
			
			BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String responseHeaders = "";
			String responseBody = "";
			String r;
			
			if((r = br.readLine()) != null) {
				responseHeaders += r.trim();
			}
			
			while((r = br.readLine()) != null) {
				responseBody += r + "\n";
			}
			
			//System.out.println("Response Headers: " + responseHeaders);
			//System.out.println("Response Body: " + responseBody);
			
			br.close();

			socket.close();
			socket = null;
			if (responseHeaders.toLowerCase().contains("success")) {
//				System.out.println("Successfully sent and received command");
				updateSuccessful = true;
			}
			
//			pw.close();
			return responseBody;
		} catch (IOException e) {
			System.out.println("IOException in sendMessage()");
			e.printStackTrace();
			return "IOException";
		}
	}
	
	private void resetUpdate() {
		this.updateSuccessful = false;
	}
	
	@Override
	public void run() {
		while(true) {
			try {
				Message m = queue.take();
				resetUpdate();
				String result = sendQuery(m.getCommand(), m.getType(), m.getIp(), m.getPort());
				result = result.trim();
				ClusterManager managerInstance = ClusterManager.getInstance();
				managerInstance.recordNodeResponse(m.getJobId(), result, m.getNodeNum(), this.updateSuccessful);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			catch (IOException e) {
				e.printStackTrace();
			}
			catch (JSONException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	
}
