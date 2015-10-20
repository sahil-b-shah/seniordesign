package UserInterface;

import java.io.IOException;
import java.sql.SQLException;

import org.json.JSONException;

import Commands.Commands;

public class SQLParser {

	private boolean debug;
	private String insertPattern = "(INSERT INTO\\s[\\s\\w]+VALUES\\s[\\s\\w]+)";
	private String selectPattern = "(SELECT\\s[\\s\\w]+FROM\\s[\\s\\w]+)";

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
			if(debug){
				System.out.println(cmd + " is a JOIN");
				return true;
			}
			return Commands.join();
		}

		else if(cmd.matches(insertPattern)){
			if(debug){
				System.out.println(cmd + " is a INSERT INTO ...");
				return true;
			}
			return Commands.insert(cmd);
		}

		else if(cmd.startsWith("CREATE TABLE")){
			if(debug){
				System.out.println(cmd + " is a CREATE TABLE");
				return true;
			}
			return Commands.createTable(cmd);
		}

		else if(cmd.startsWith("CREATE DB")){
			if(debug){
				System.out.println(cmd + " is a DELETE FROM ... INTO ...");
				return true;
			}
			return Commands.delete(cmd);
		}


		else if(cmd.startsWith("DELETE FROM")){
			if(debug){
				System.out.println(cmd + " is a DELETE FROM ... INTO ...");
				return true;
			}
			return Commands.delete(cmd);
		}

		else if(cmd.matches(selectPattern)){
			if(debug){
				System.out.println(cmd + " is a SELECT");
				return true;
			}
			return Commands.select(cmd);
		}
		else{
			System.out.println(cmd + " is not a recognized command");
			return false;
		}
	}

}
