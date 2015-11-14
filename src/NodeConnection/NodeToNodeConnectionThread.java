package NodeConnection;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import NodeController.DBInstance;

public class NodeToNodeConnectionThread extends Thread {
	
	private BlockingQueue<NodeMessage> queue;
	private DBInstance db;
	private String masterIP;
	private int masterPort;
	private Map<String, Integer> nodeAddrs;
	private String curNodeIP;
	private int curNodePort; 
	
	public NodeToNodeConnectionThread(DBInstance db, BlockingQueue<NodeMessage> q,
			String masterIP, int masterPort, Map<String, Integer> nodeAddrs,
			String curNodeIP, int curNodePort) {
		this.db = db;
		this.queue = q;
		this.masterIP = masterIP;
		this.masterPort = masterPort;
		this.nodeAddrs = nodeAddrs;
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
				req += line;
			}
		}
		
		String retMessage = "";

		try {
			if (type.equals("UPDATE")) {
				if(db.runMySQLUpdate(req) == 0) {
					retMessage += "Success\r\n\r\n";
				}
				else {
					retMessage += "Failure\r\n\r\n";
				}
			}
			else if(type.equals("QUERY")) {
				/*
				 * IMPORTANT: here is where you would check if it is a JOIN
				 * and process differently. You can open a socket to
				 * another node (the ip and port combinations are in NodeAddr)
				 * If you're sending stuff to a node indexed by a number,
				 * pass in a map from integer to ip from NodeManager when
				 * thread is created (you would initialize and populate the map
				 * in initializeNodes() in NodeManager)
				 */
				retMessage += "Success\r\n\r\n" + db.runMySQLQuery(req);
			}
		} catch (SQLException e) {
			System.out.println("SQLException while executing command " + req);
			return;
		}
		
		
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()));
		
		out.write(retMessage);
		out.flush();
	}

	@Override
	public void run() {
		while (true) {
			try {
				NodeMessage nodeMessage = this.queue.take();
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
