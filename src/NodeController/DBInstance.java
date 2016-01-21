package NodeController;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBInstance {

	private Connection connection;
	
	public DBInstance(String address, int port, int dbInstance) throws ClassNotFoundException, SQLException{
		Class.forName("com.mysql.jdbc.Driver");
		Properties properties = new Properties();
		properties.put("user", "cis400");
		properties.put("password", "cis400ad");
		System.out.println("Trying to establish connection to db");
		connection = DriverManager.getConnection("jdbc:mysql://" + address +
				":" + port + "/database" + dbInstance, "cis400", "cis400ad");
		System.out.println("Established connection to db");
	}
	
	public int runMySQLUpdate(String update) throws SQLException{		
		Statement statement = null;
		statement = connection.createStatement();
		int result = statement.executeUpdate(update);
		statement.close();
		return result;
	}
	
	public String runMySQLQuery(String query) throws SQLException{		
		Statement statement = null;
		ResultSet result = null;
		statement = connection.createStatement();
		result = statement.executeQuery(query);
		
		ResultSetMetaData rsmd = result.getMetaData();
		int columns = rsmd.getColumnCount();
		
		System.out.println("Columns: " + columns);
		for (int i = 1; i <= columns; i++) {
			System.out.println("Column " + i + ": " + rsmd.getColumnLabel(i));
		}

		String res = "";
		while (result.next()) {
			for (int i = 1; i <= columns; i++) {
				res += result.getString(i) + ",";
			}
		}
		
		statement.close();
		return res.substring(0, res.length() - 1);
	}
}
