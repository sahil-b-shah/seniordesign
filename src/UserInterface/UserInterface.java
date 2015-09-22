package UserInterface;

import java.sql.SQLException;
import java.util.Scanner;

import Manager.DBManager;

public class UserInterface {

	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		SQLParser parser = new SQLParser();

		//TODO change this print statement
		System.out.println("Using config file in location (blank)");

		System.out.println("\nPrint MySQL Command (q to Quit)");

		String command = "";
		while(true){
			command = scanner.next();

			if(command.equalsIgnoreCase("q"))
				break;  //program quit

			while(scanner.hasNext()){				
				command = command + " " + scanner.next();
			}

			try {
				if(parser.parse(command))
					System.out.println("\nCommand execueted. Print another MySQL Command (q to Quit):");
				else
					System.out.println("\nThat was an invalid command. Print another MySQL Command (q to Quit):");
			} catch (SQLException e) {
				System.out.println("\nSomething went wrong. Print another MySQL Command (q to Quit):");

			}
		}

		scanner.close();



	}

}
