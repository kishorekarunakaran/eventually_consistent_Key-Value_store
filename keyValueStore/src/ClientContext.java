import java.net.Socket;
import java.util.HashMap;

public class ClientContext {
	
	private String coordinator = null;
	static String coIp = null;
	static int coPort = 0;
	static Socket sock = null;

	public ClientContext(String coor) {
		coordinator = coor;
	}
	
	public void readFile(FileProcessor fp) {
		
		while(true){
			String value = fp.readLine();
			if(value == null){
				break;	
			}
			String[] splitValue;
			splitValue = value.split(" ");
			if(coordinator.equalsIgnoreCase(splitValue[0])) {
				coIp = splitValue[1];
				coPort = Integer.parseInt(splitValue[2]);
			}		
		}

		System.out.println(coIp + " " + coPort);
	}
		
}
