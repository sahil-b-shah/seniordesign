package Manager;

import java.util.HashMap;

public class ClusterManagerCheckStatusThread extends Thread{

	public static HashMap<String, Long> statusMap;
	
	public ClusterManagerCheckStatusThread(HashMap<String, Long> map){
		statusMap = map;
	}
	
	public void run() {
		while(true){
			try {
				detectError();
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static boolean detectError(){
		
		for(String ip: statusMap.keySet()){
			long time = statusMap.get(ip);
			if(System.currentTimeMillis() < (time + 30000)){
				fixSystem();
				return false;
			}
		}
		return true;
	}
	
	private static void fixSystem(){
		//TODO: fix system if error detected
	}
}
