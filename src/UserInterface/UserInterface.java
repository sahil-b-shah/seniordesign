package UserInterface;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

import org.json.JSONException;

import Manager.ClusterManager;
import Manager.ClusterManagerCheckStatusThread;

public class UserInterface {

	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		SQLParser parser = new SQLParser(false);
		ClusterManager manager = null;
		
		try {
			manager = ClusterManager.getInstance();   //starts cluster manager
		} catch (Exception e){
			System.out.println("Cluster Manager could not start. Quitting...");
			scanner.close();
			return;
		}
		
		System.out.println("Starting user interface...");
		while(!manager.ready()){ //Wait until all nodes respond;
			try {
				System.out.println("Waiting for all nodes to respond");
				Thread.sleep(5000);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}

		System.out.println("\nPrint MySQL Command (Type 'exit' or 'quit' to end program)");

		String command = "";
		while(scanner.hasNextLine()){
			command = scanner.nextLine();

			if(command.equalsIgnoreCase("q") || command.equalsIgnoreCase("quit") 
						|| command.equalsIgnoreCase("exit"))
				break;  //program quit

			try {
				ClusterManagerCheckStatusThread.detectError();   //make sure nodes ready to go
				if(parser.parse(command))
					System.out.println("\nCommand executed. Print another MySQL Command  (Type 'exit' or 'quit' to end program)");
				else
					System.out.println("\nThat was an invalid command. Print another MySQL Command  (Type 'exit' or 'quit' to end program)");
			} catch (SQLException e) {
				System.out.println("\nSomething went wrong. Print another MySQL Command  (Type 'exit' or 'quit' to end program)");
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("\nThere was an error opening the config files or creating Cluster socket. Exiting");
				e.printStackTrace();
				break;
			} catch (JSONException e) {
				System.out.println("\nThere was an error parsing the config files. Exiting");
				e.printStackTrace();
				break;
			}
		}
		scanner.close();
	}

}
