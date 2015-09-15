package UserInterface;

import java.util.Scanner;

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
			
			if(parser.parse(command))
				System.out.println("\nCommand execueted. Print another MySQL Command (q to Quit):");
			else
				System.out.println("\nThat was an invalid command. Print another MySQL Command (q to Quit):");
		}
		
		scanner.close();



	}

}
