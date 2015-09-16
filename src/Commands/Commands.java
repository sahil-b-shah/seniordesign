package Commands;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import Manager.DBManager;

public class Commands {
	
	public static boolean join(){
		//TODO: Implement join
		return false;
	}	
	
	public static boolean runMySQLCommand(String query) throws SQLException{		
		//TODO: sample code that may work below, don't run it
		Statement statement = null;
		DBManager manager = DBManager.getInstance();
		Connection connection = manager.getConnection();
		try {
			statement = connection.createStatement();
			//TODO: figure out what to do with results
			ResultSet result = statement.executeQuery(query);
			statement.close();
		} catch (SQLException e) {
			//TODO: come up with common format for printing stack traces
			e.printStackTrace();
			statement.close();
			return false;
		}
		return true;
	}
}
