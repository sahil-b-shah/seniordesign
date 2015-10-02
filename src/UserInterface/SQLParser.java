package UserInterface;

import java.io.IOException;
import java.sql.SQLException;

import org.json.JSONException;

import Commands.Commands;

public class SQLParser {

	private boolean debug;
	private String insertPattern = "(INSERT INTO\\s[\\s\\w]+VALUES\\s[\\s\\w]+)";
	
	public SQLParser(boolean mode){
		debug = mode;
	}

	/**
	 * Parses command string and executes command
	 * @param cmd - command to execute
	 * @return true if command executed, and false if invalid command
	 * @throws SQLException 
	 * @throws JSONException 
	 * @throws IOException 
	 */
	public boolean parse(String cmd) throws SQLException, IOException, JSONException{
		if(cmd.startsWith("JOIN")){
			System.out.println(cmd + " is a JOIN");
			if(debug) return true;
			return Commands.join();
		}
		
		else if(cmd.matches(insertPattern)){
			System.out.println(cmd + " is a INSERT INTO ...");
			if(debug) return true;
			return Commands.insert(cmd);
		}
		
		else if(cmd.startsWith("CREATE TABLE")){
			System.out.println(cmd + " is a CREATE TABLE");
			if(debug) return true;
			return Commands.createTable(cmd);
		}
		
		else if(cmd.startsWith("CREATE DB")){
			System.out.println(cmd + " is a DELETE FROM ... INTO ...");
			if(debug) return true;
			return Commands.delete(cmd);
		}
		
		
		else if(cmd.startsWith("DELETE FROM")){
			System.out.println(cmd + " is a DELETE FROM ... INTO ...");
			if(debug) return true;
			return Commands.delete(cmd);
		}
		
		else if(cmd.startsWith("SELECT")){
			System.out.println(cmd + " is a SELECT");
			if(debug) return true;
			return Commands.select(cmd);
		}
		
		
		
		else{
			System.out.println(cmd + " is not a recognized command");
			return false;
		}
	}

}
