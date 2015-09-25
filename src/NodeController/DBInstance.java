package NodeController;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBInstance {

	private static Connection connection;
	
	public DBInstance(String address, String port){
		connection = null;
	}
	
	public static int runMySQLUpdate(String update) throws SQLException{		
		Statement statement = null;
		statement = connection.createStatement();
		int result = statement.executeUpdate(update);
		statement.close();
		return result;
	}
	
	public static ResultSet runMySQLQuery(String query) throws SQLException{		
		Statement statement = null;
		ResultSet result = null;
		statement = connection.createStatement();
		result = statement.executeQuery(query);
		statement.close();
		return result;
	}
}
