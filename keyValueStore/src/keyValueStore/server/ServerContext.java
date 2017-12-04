package keyValueStore.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import keyValueStore.keyValue.KeyValue;
import keyValueStore.util.FileProcessor;

public class ServerContext {
	
	private String name = null;
	static ServerSocket server;
	static int port = 0;
	private int flag = 0;
	
	//maintains status of the replica servers value = true -> connected,false -> not connectedd
	private HashMap<String,Boolean> connectedServers = new HashMap<String,Boolean>();
	private HashMap<String,Boolean> hintServers = new HashMap<String,Boolean>();
	
	//map<serverNames,Ip> to store all ip addresses of servers
	static HashMap<String,String> serversIp = new HashMap<String,String>();
	
	//map<serverNames,port> to store all port numbers of servers
	static HashMap<String,Integer> serversPort = new HashMap<String,Integer>();
	
	//In memory data structure to store key value pairs	
	static HashMap<Integer,KeyValue.KeyValuePair> store = new HashMap<Integer,KeyValue.KeyValuePair>();
	
	//stores a list of put keys for a crashed server
	public HashMap<String, ArrayList<KeyValue.HintedHandoff> > hintedHandoffMap = new HashMap<String, ArrayList<KeyValue.HintedHandoff>>();
	
	public ServerContext(String nameIn, int portIn) {
		setName(nameIn);
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
	
	public void readLog(FileProcessor fp) {
		
		while(true) {
			String line = fp.readLine();
			if(line == null) {
				break;
			}
			String [] splitValue;
			splitValue = line.split(" ");
			int key = Integer.parseInt(splitValue[0]);
			
			KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();			
			keyStore.setKey(key);
			keyStore.setValue(splitValue[1]);
			keyStore.setTime(Long.parseLong(splitValue[2]));
			
			if(store.containsKey(key)) {
				store.replace(key, keyStore.build());
			}
			else {
				store.put(key, keyStore.build());
			}
		}
	}

	public void printStore() {
		for(int key: store.keySet()) {		
			System.out.println(store.get(key).getKey() + "   " + store.get(key).getValue() + "   " + store.get(key).getTime());
		}
	}

	public HashMap<String,Boolean> getConnectedServers() {
		return connectedServers;
	}
	
	public synchronized boolean containsServer(String name) {
		return connectedServers.containsKey(name);
	}
	
	public synchronized boolean getServerStatus(String name) {
		return connectedServers.get(name);
	}

    //Adds the state of the connected server...true -> connected, false -> not connected
	public synchronized void addConnectedServers(String serverName, Boolean b) {
		
		if(connectedServers.containsKey(serverName)) {
			connectedServers.replace(serverName, b);
		}
		else {
			connectedServers.put(serverName, b);
		}
	}
	
	//returns the number of servers connected
	public synchronized int getCountConnectedServers() {
		int value = 0;
		for(String name: connectedServers.keySet()) {
			if(connectedServers.get(name) == true) {
				value++;
			}
		}		
		return value;
	}

	public synchronized boolean containshintServer(String name) {
		return hintServers.containsKey(name);
	}
	
	public synchronized boolean gethintStatus(String name) {
		return hintServers.get(name);
	}

    //Adds the state of the connected server...true -> connected, false -> not connected
	public synchronized void addhintServers(String serverName, Boolean b) {
		
		if(hintServers.containsKey(serverName)) {
			hintServers.replace(serverName, b);
		}
		else {
			hintServers.put(serverName, b);
		}
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	
	public void hintedHandoff(String name) {
		
		//loops over the hintedHandoffMap hashmap, matches the server name..if exists, send the key to that server
		if(hintedHandoffMap.containsKey(name)) {
			ArrayList<KeyValue.HintedHandoff> ls = hintedHandoffMap.get(name);
				
				for(KeyValue.HintedHandoff temp : ls) {
					
					KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
				
					keyMessage.setConnection(0);
					keyMessage.setHintedHandoff(temp);
				
					Socket socket;
					try {
						socket = new Socket(serversIp.get(name), serversPort.get(name));
						OutputStream out = socket.getOutputStream();
						keyMessage.build().writeDelimitedTo(out);
						out.flush();
						socket.close();
						
					} catch (UnknownHostException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}				
				}
		
		}
		hintedHandoffMap.remove(name);
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flagIn) {
		this.flag = flagIn;
	}
}
