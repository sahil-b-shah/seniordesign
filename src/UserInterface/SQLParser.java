package UserInterface;

import java.sql.SQLException;

import Commands.Commands;

public class SQLParser {

	public SQLParser(){}

	/**
	 * Parses command string and executes command
	 * @param cmd - command to execute
	 * @return true if command executed, and false if invalid command
	 * @throws SQLException 
	 */
	public boolean parse(String cmd) throws SQLException{
		if(cmd.startsWith("JOIN")){
			System.out.println(cmd + " is a JOIN");
			return Commands.join();
		}
		
		else if(cmd.startsWith("INSERT INTO")){
			System.out.println(cmd + " is a INSERT INTO ...");
			return Commands.insert(cmd);
		}
		
		else if(cmd.startsWith("CREATE TABLE")){
			System.out.println(cmd + " is a CREATE TABLE");
			return Commands.createTable(cmd);
		}
		
		else if(cmd.startsWith("DELETE FROM")){
			System.out.println(cmd + " is a DELETE FROM ... INTO ...");
			return Commands.delete(cmd);
		}
		
		else if(cmd.startsWith("SELECT")){
			System.out.println(cmd + " is a SELECT");
			return Commands.select(cmd);
		}
		
		
		
		else{
			System.out.println(cmd + " is not a recognized command");
			return false;
		}
	}

}
