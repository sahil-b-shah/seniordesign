package Manager;

import java.io.IOException;
import java.util.HashMap;

import org.json.JSONException;

import Utilities.NodeStatus;

public class ClusterManagerCheckStatusThread extends Thread{

	public static HashMap<String, Long> statusMap;
	
	public ClusterManagerCheckStatusThread(HashMap<String, Long> map){
		statusMap = map;
	}
	
	public void run() {
		while(true){
			try {
				detectError();
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static boolean detectError(){
		boolean errors = false;
		for(String address: statusMap.keySet()){
			long time = statusMap.get(address);
			if(System.currentTimeMillis() > (time + 10000)){
				errors = true;
				try {
					ClusterManager.getInstance().setNodeStatus(address, NodeStatus.FAILED);
				} catch (IOException | JSONException e) {
					e.printStackTrace();
				}
			}
		}
		return errors;
	}

}
