/* This Object is not being used. The functionality of this object was converted into a 
 thread (used to create a thread pool in ClusterManager and NodeManager).
 
 Still around as a reference for implementing NodeConnectionThread
*/

package MessageConnection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.ResultSet;

public class MessageConnection {

	private Socket socket;
	private boolean updateSuccessful;
	private String latestResult;
	private String address;
	private int port;
	
	public MessageConnection(String addr, int p){
		port = p;
		address = addr;
		updateSuccessful = false;
		latestResult = null;
	}

	private Socket getSocket() throws UnknownHostException, IOException{
		return new Socket(address, port);
	}
	
	public boolean sendMessage(String query, String type){
		try {
			socket = getSocket();
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
				this.updateSuccessful = true;
			}
			this.latestResult = response; // store ResultSet instead
			
//			pw.close();
			return true;
		} catch (IOException e) {
			System.out.println("IOException in sendMessage()");
			return false;
		}
	}
	
	public void resetUpdate() {
		this.updateSuccessful = false;
	}
	
	public ResultSet getResultSet(){
		//returns null if error returned(invalid query, etc
		return null;
	}
	
	public String getResultString(){
		if(latestResult.isEmpty())
			return null;
		else
			return latestResult;
	}

	public boolean updateSuccessful() {
		// TODO Auto-generated method stub
		return this.updateSuccessful;
	}
	
	
}
