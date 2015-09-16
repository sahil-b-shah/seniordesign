package Manager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBManager {
	
	private String user;
	private String password;
	private String server;
	private String port;
	private static DBManager managerInstance;
	
	public static DBManager getInstance(){
		if(managerInstance == null)
			managerInstance = new DBManager();
		
		//TODO: set password, server, etc
		
		return managerInstance;
	}
	
	public Connection getConnection() throws SQLException {	
		Connection connection = null;
		Properties properties = new Properties();
		properties.put("user", user);
		properties.put("password",password);
		
		connection = DriverManager.getConnection("jdbc:mysql://" + server +
				":" + port + "/",properties);
		
		return connection;
		
	}
}
