import java.net.ServerSocket;
import java.util.HashMap;

public class ServerContext {
	
	private String name = null;
	static ServerSocket server;
	static int port = 0;
	
	//map<serverNames,Ip> to store all ip addresses of servers
	static HashMap<String,String> serversIp = new HashMap<String,String>();
	
	//map<serverNames,port> to store all port numbers of servers
	static HashMap<String,Integer> serversPort = new HashMap<String,Integer>();
	
	//In memory data structure to store key value pairs
	static HashMap<Integer,KeyStore> store = new HashMap<Integer,KeyStore>();
	
	public ServerContext(String nameIn, int portIn) {
		name = nameIn;
		port = portIn;
	}

	public void readFile(FileProcessor fp) {
		
		while(true){
			String value = fp.readLine();
			if(value == null){
				break;	
			}
			String[] splitValue;
			splitValue = value.split(" ");
			serversIp.put(splitValue[0], splitValue[1]);
			serversPort.put(splitValue[0], Integer.parseInt(splitValue[2]));
		}
	}

	public void printStore() {
		for(int key: store.keySet()) {
			System.out.println(store.get(key).getKey() + " " + store.get(key).getValue() + " " + store.get(key).getTimestamp());
		}
	}
	
}
