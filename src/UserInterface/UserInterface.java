package UserInterface;

import java.sql.SQLException;
import java.util.Scanner;

public class UserInterface {

	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		SQLParser parser = new SQLParser(false);

		//TODO change this print statement
		System.out.println("Using config file in location (blank)");

		System.out.println("\nPrint MySQL Command (Type 'exit' or 'quit' to end program)");

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
					System.out.println("\nCommand execueted. Print another MySQL Command  (Type 'exit' or 'quit' to end program)");
				else
					System.out.println("\nThat was an invalid command. Print another MySQL Command  (Type 'exit' or 'quit' to end program)");
			} catch (SQLException e) {
				System.out.println("\nSomething went wrong. Print another MySQL Command  (Type 'exit' or 'quit' to end program)");

			}
		}

		scanner.close();



	}

}
