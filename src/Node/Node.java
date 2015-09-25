package Node;

import java.io.IOException;
import java.net.Socket;
import java.sql.ResultSet;

public class Node {

	private Socket socket;
	
	public Node(String address, int port){
		try {
			socket =  new Socket(address, port);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public Socket getSocket(){
		return socket;
	}
	
	public boolean sendMessage(String query){
		return false;
	}
	
	public ResultSet getResultSet(){
		//returns null if error returned(invalid query, etc)
		return null;
	}

	public boolean updateSuccessful() {
		// TODO Auto-generated method stub
		return false;
	}
	
	
}
