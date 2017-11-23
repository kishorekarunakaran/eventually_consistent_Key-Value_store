import java.net.Socket;
import java.util.HashMap;

public class ClientContext {
	
	public String coordinator = null;
	public String coIp = null;
	public int coPort = 0;
	public Socket sock = null;

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
				this.coIp = splitValue[1];
				this.coPort = Integer.parseInt(splitValue[2]);
			}		
		}

		System.out.println(coIp + " " + coPort);
	}
		
	public void setSocket(Socket in) {
		this.sock = in;
	}
	
	public Socket getSocket() {
		return this.sock;
	}
	
}
