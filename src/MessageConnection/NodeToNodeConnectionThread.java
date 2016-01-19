package MessageConnection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import NodeController.DBInstance;
import NodeController.NodeManager;

public class NodeToNodeConnectionThread extends Thread {
	
	private BlockingQueue<Message> queue;
	private String masterIP;
	private int masterPort;
	private Map<String, Integer> nodeAddrs;
	private String curNodeIP;
	private int curNodePort; 
	
	public NodeToNodeConnectionThread(BlockingQueue<Message> q,
			String masterIP, int masterPort,
			String curNodeIP, int curNodePort) {
		this.queue = q;
		this.masterIP = masterIP;
		this.masterPort = masterPort;
		this.curNodeIP = curNodeIP;
		this.curNodePort = curNodePort;
	}
	
	private void processMessageFromMaster(Socket s) throws IOException{
		BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
		String req = "";
		String type = "";
		String line;
		
		boolean firstEmpty = true;
		boolean first = true;
		
		while((line = in.readLine()) != null) {
			System.out.println("Line: " + line);
			if (line.trim().equals("") && !firstEmpty) {
				break;
			}
			else if (line.trim().equals("")) {
				firstEmpty = true;
			}
			else {
				firstEmpty = false;
			}
			
			if (first) {
				first = false;
				type = line;
			}
			else {
				req += line + "\n";
			}
		}
		
		String retMessage = "";
		System.out.println("Type: " + type);
		System.out.println("Req: " + req);
		try {
			if (type.equals("UPDATE")) {
				DBInstance db = NodeManager.getDB();
				if(db.runMySQLUpdate(req) == 0) {
					retMessage += "Success\r\n\r\n";
				}
				else {
					retMessage += "Failure\r\n\r\n";
				}
			}
			else if (type.equals("QUERY")) {
				/*
				 * IMPORTANT: here is where you would check if it is a JOIN
				 * and process differently. You can open a socket to
				 * another node (the ip and port combinations are in NodeAddr)
				 * If you're sending stuff to a node indexed by a number,
				 * pass in a map from integer to ip from NodeManager when
				 * thread is created (you would initialize and populate the map
				 * in initializeNodes() in NodeManager)
				 */
				DBInstance db = NodeManager.getDB();
				retMessage += "Success\r\n\r\n" + db.runMySQLQuery(req);
				System.out.println("retMessage " + retMessage);
			}
			else if (type.equals("STATUS")) {
				String[] lines = req.split("\r|\n|\r\n");
				for (int i = 0; i < lines.length; i++) {
					if (lines[i].startsWith("db_addr")) {
						//dbInstance[0] will be the actual db addr, while dbInstance[1] will be the db instance number
						String[] dbInstance = lines[i].split("/dbinstance=");
						// Splitting the db addr for the db ip and port
						String[] dbData = dbInstance[0].split("=")[1].trim().split(":");

//						System.out.println("DB IP: " + dbData[0]);
//						System.out.println("DB Port: " + dbData[1]);
//						System.out.println("DB Num: " + dbInstance[1]);
						DBInstance db = new DBInstance(dbData[0], Integer.parseInt(dbData[1]), Integer.parseInt(dbInstance[1]));
						NodeManager.setDB(db);
					}
					else if (lines[i].startsWith("nodes")) {
						String[] nodeData = lines[i].split("=")[1].trim().split(";");
						nodeAddrs = new HashMap<String, Integer>();
						for (int nd = 0; nd < nodeData.length; nd++) {
							String[] node = nodeData[nd].split(":");
							nodeAddrs.put(node[0], Integer.parseInt(node[1]));
						}
					}
				}
				
				retMessage += "READY\r\n\r\n";
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
			System.out.println("SQLException while executing command " + req);
			return;
		}
		catch (NumberFormatException e) {
			e.printStackTrace();
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
		System.out.println(retMessage);
		out.write(retMessage);
		out.flush();
	}

	@Override
	public void run() {
		while (true) {
			try {
				Message nodeMessage = this.queue.take();
				Socket s = nodeMessage.getSocket();
				
				if (nodeMessage.isMessageFromMaster()) {
					processMessageFromMaster(s);
				}
				else {
					/*
					 * Message is from another node (possibly as a result
					 * of a JOIN). Process it differently (add another private
					 * method)
					 */
				}
				
				s.close();
	
				System.out.println("-------\n");
			}
			catch(IOException e) {
				System.out.println("IOException in NodeToNodeConnectionThread");
				e.printStackTrace();
			}
			catch (InterruptedException e1) {
				System.out.println("InterrupedException while trying to take() from the blocking Q in NodeToNodeConnectionThread");
				e1.printStackTrace();
			}
		}
	}

}
