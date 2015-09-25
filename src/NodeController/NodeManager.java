package NodeController;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class NodeManager {

	private static DBInstance db;
	
	public static void main(String args[]){
		System.out.println("Initializing Node Connection. Waiting for messges.");
		db = null;
		//TODO: needs to constantly listen to socket and send queries based on commands from master 
		
	}
	
	public static ResultSet runMySQLCommand(String query) throws SQLException{		
		Statement statement = null;
		ResultSet result = null;
		Connection connection = db.getConnection();
		statement = connection.createStatement();
		result = statement.executeQuery(query);
		statement.close();
		return result;
	}
	
}
