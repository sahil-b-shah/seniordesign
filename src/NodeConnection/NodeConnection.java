package NodeConnection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.ResultSet;

public class NodeConnection {

	private Socket socket;
	private boolean updateSuccessful;
	private String latestResult;
	private String address;
	private int port;
	
	public NodeConnection(String addr, int p){
		port = p;
		address = addr;
		updateSuccessful = false;
		latestResult = null;
	}

	private Socket getSocket() throws UnknownHostException, IOException{
		return new Socket(address, port);
	}
	
	public boolean sendMessage(String query){
		try {
			socket = getSocket();
			PrintWriter pw = new PrintWriter(socket.getOutputStream());
			pw.println(query);
			pw.flush();
			pw.close();
			
//			BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//			String response = "";
//			String r;
//			while((r = br.readLine()) != null) {
//				response += r + "\n";
//			}
//			System.out.println("Response " + response);
//			br.close();
//			socket.close();
//			socket = null;
//			if (response.toLowerCase().contains("success")) {
//				System.out.println("Successfully sent and received command");
//				this.updateSuccessful = true;
//			}
//			this.latestResult = response; // store ResultSet instead
			return true;
		} catch (IOException e) {
			System.out.println("IOException in sendMessage()");
			return false;
		}
	}
	
	public ResultSet getResultSet(){
		//returns null if error returned(invalid query, etc)
		return null;
	}

	public boolean updateSuccessful() {
		// TODO Auto-generated method stub
		return this.updateSuccessful;
	}
	
	
}
