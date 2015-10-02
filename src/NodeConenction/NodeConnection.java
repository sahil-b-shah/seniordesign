package NodeConenction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.ResultSet;

public class NodeConnection {

	private Socket socket;
	private boolean updateSuccessful;
	private String latestResult;
	
	public NodeConnection(String address, int port){
		try {
			socket =  new Socket(address, port);
			updateSuccessful = false;
			latestResult = null;
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public Socket getSocket(){
		return socket;
	}
	
	public boolean sendMessage(String query){
		try {
			PrintWriter pw = new PrintWriter(socket.getOutputStream());
			pw.println(query);
			pw.flush();
			BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String response = "";
			String r;
			while((r = br.readLine()) != null) {
				response += r + "\n";
			}
			br.close();
			if (response.toLowerCase().contains("success")) {
				this.updateSuccessful = true;
			}
			this.latestResult = response; // store ResultSet instead
			return true;
		} catch (IOException e) {
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
