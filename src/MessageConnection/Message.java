package MessageConnection;

import java.net.Socket;

public class Message {
	
	private Socket socket;
	private boolean messageFromMaster;

	public Message(Socket s, boolean messageFromMaster) {
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
