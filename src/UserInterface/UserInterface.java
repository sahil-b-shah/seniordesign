package UserInterface;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

import org.json.JSONException;

import Manager.ClusterManager;

public class UserInterface {

	public static void main(String[] args) {
		try {
			ClusterManager.initNodes();  // init ClusterManager and connection to all nodes
		} 
		catch (IOException e1) {
			System.out.println("IOException while initializing ClusterManager. Quitting");
			e1.printStackTrace();
			return;
		} 
		catch (JSONException e1) {
			System.out.println("JSONException while initializing ClusterManager. Quitting");
			e1.printStackTrace();
			return;
		}
		
		Scanner scanner = new Scanner(System.in);
		SQLParser parser = new SQLParser(false);
		
		System.out.println("Starting user interface...");
		System.out.println("\nPrint MySQL Command (Type 'exit' or 'quit' to end program)");

		String command = "";
		while(scanner.hasNextLine()){
			command = scanner.nextLine();

			if(command.equalsIgnoreCase("q") || command.equalsIgnoreCase("quit") 
						|| command.equalsIgnoreCase("exit"))
				break;  //program quit

			try {
				//ClusterManagerCheckStatusThread.detectError();   //make sure nodes ready to go
				if(parser.parse(command))
					System.out.println("\nCommand execueted. Print another MySQL Command  (Type 'exit' or 'quit' to end program)");
				else
					System.out.println("\nThat was an invalid command. Print another MySQL Command  (Type 'exit' or 'quit' to end program)");
			} catch (SQLException e) {
				System.out.println("\nSomething went wrong. Print another MySQL Command  (Type 'exit' or 'quit' to end program)");

			} catch (IOException e) {
				System.out.println("\nThere was an error opening the config files or creating Cluster socket. Exiting");
				break;
			} catch (JSONException e) {
				System.out.println("\nThere was an error parsing the config files. Exiting");
				break;
			}
		}
		scanner.close();
	}

}
