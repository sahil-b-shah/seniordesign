package Manager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBNode {

	private Properties properties;
	private String server;
	private String port;
	
	public DBNode(String user, String password, String server, String port){
		properties = new Properties();
		properties.put("user", user);
		properties.put("password",password);
		this.port = port;
		this.server = server;
		
	}
	
	public Connection getConnection() throws SQLException {	
		Connection connection = null;
		connection = DriverManager.getConnection("jdbc:mysql://" + server +
				":" + port + "/",properties);
		
		return connection;
		
	}
}
