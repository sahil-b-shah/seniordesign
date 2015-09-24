package Commands;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import Manager.DBManager;
import Manager.DBNode;

public class Commands {

	public static boolean join(){
		//TODO: Implement join
		return false;
	}	

	public static ResultSet runMySQLCommand(String query, DBNode node) throws SQLException{		
		//TODO: sample code that may work below, don't run it
		Statement statement = null;
		ResultSet result = null;
		Connection connection = node.getConnection();
		statement = connection.createStatement();
		result = statement.executeQuery(query);
		statement.close();
		return result;
	}

	public static boolean insert(String cmd) {
		// TODO Auto-generated method stub
		return false;
	}

	/**
	 * Creates a distributed table
	 * @param cmd: query from parser that starts with "CREATE TABLE"
	 * @return true if worked, else false
	 */
	public static boolean createTable(String cmd) {
		for(DBNode node: DBManager.getNodes()){
			try {
				//create same new table on each node (same command for each node)
				runMySQLCommand(cmd, node);
			} catch (SQLException e) {
				e.printStackTrace();
				return false;   //error in command
			}
		}
		return true;   //no SQL exception anywhere
	}

	public static boolean delete(String cmd) {
		// TODO Auto-generated method stub
		return false;
	}

	public static boolean select(String cmd) {
		// TODO Auto-generated method stub
		return false;
	}
}
