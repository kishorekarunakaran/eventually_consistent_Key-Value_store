import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;

public class ServerContext {
	
	public String name = null;
	public ServerSocket server;
	public int port = 0;
	public HashMap<String,String> serversIp = new HashMap<String,String>();
	public HashMap<String,Integer> serversPort = new HashMap<String,Integer>();
	public HashMap<Integer,KeyStore> store = new HashMap<Integer,KeyStore>();
	
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
			System.out.println(store.get(key).key + " " + store.get(key).value + " " + store.get(key).timestamp);
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
			KeyValue.KeyValueMessage.Builder km = KeyValue.KeyValueMessage.newBuilder();
			km.setConnection(0);
			km.setPutKey(putserver).build();
			sendToServers(km);
		}
	}

	public void sendToServers(KeyValue.KeyValueMessage.Builder in) {
		
		for(String temp: serversIp.keySet()) {
			try {
				Socket send = new Socket(serversIp.get(temp), serversPort.get(temp));
				OutputStream out = send.getOutputStream();
				in.build().writeDelimitedTo(out);
				out.flush();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
}
