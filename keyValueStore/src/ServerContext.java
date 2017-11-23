import java.net.ServerSocket;
import java.util.Date;
import java.util.HashMap;

public class ServerContext {
	
	public String name = null;
	public ServerSocket server;
	public int port = 0;
	public HashMap<String,String> serversIp = new HashMap<String,String>();
	public HashMap<String,Integer> serversPort = new HashMap<String,Integer>();
	
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
			serversPort.put(splitValue[0], Integer.parseInt(splitValue[1]));
		}
	}

	public void handleClient(KeyValue.KeyValueMessage incoming) {
		if(incoming.hasPutKey()) {
			KeyValue.Put put = null;
			put = incoming.getPutKey();
			//System.out.println(put.getKey() + " " + put.getValue() + " " + put.getConsistency());
			KeyValue.Put.Builder putserver = KeyValue.Put.newBuilder();
			putserver.setKey(put.getKey());
			putserver.setValue(put.getValue());
			putserver.setConsistency(put.getConsistency());
			Date date = new Date();
			long time = date.getTime();
			putserver.setTime(time);
		}
	}
	
}
