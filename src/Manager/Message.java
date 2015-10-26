package Manager;

public class Message {
	private String command;
	private String type;
	private String ip;
	private int port;
	private int nodeNum;
	
	public Message(String cmd, String type, String ip, int port, int nodeNum) {
		this.command = cmd;
		this.type = type;
		this.ip = ip;
		this.port = port;
		this.nodeNum = nodeNum;
	}
	
	public String getCommand() {
		return this.command;
	}

	public String getType() {
		return this.type;
	}
	
	public String getIp() {
		return this.ip;
	}
	
	public int getPort() {
		return this.port;
	}
	
	public int getNodeNum() {
		return this.nodeNum;
	}
}
