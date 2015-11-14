package NodeConnection;

import java.net.Socket;

public class NodeMessage {
	
	private Socket socket;
	private boolean messageFromMaster;

	public NodeMessage(Socket s, boolean messageFromMaster) {
		this.socket = s;
		this.messageFromMaster = messageFromMaster;
	}
	
	public Socket getSocket() {
		return this.socket;
	}
	
	public boolean isMessageFromMaster() {
		return this.messageFromMaster;
	}
}
