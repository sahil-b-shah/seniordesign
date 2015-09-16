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
		switch (cmd){

		case "JOIN":{
			System.out.println(cmd + "\n is a JOIN");
			return Commands.join();
		}
		
		default:
			return Commands.runMySQLCommand(cmd);
		}
	}

}
