package UserInterface;

import Commands.Commands;

public class SQLParser {

	public SQLParser(){}

	/**
	 * Parses command string and executes command
	 * @param cmd - command to execute
	 * @return true if command executed, and false if invalid command
	 */
	public boolean parse(String cmd){
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
