package keyValueStore.server;

import java.net.ServerSocket;
import java.util.HashMap;

import keyValueStore.util.FileProcessor;

public class ServerContext {
	
	private String name = null;
	static ServerSocket server;
	static int port = 0;
	
	//maintains status of the replica servers value = true -> connected,false -> not connectedd
	private HashMap<String,Boolean> connectedServers = new HashMap<String,Boolean>();
	
	//map<serverNames,Ip> to store all ip addresses of servers
	static HashMap<String,String> serversIp = new HashMap<String,String>();
	
	//map<serverNames,port> to store all port numbers of servers
	static HashMap<String,Integer> serversPort = new HashMap<String,Integer>();
	
	//In memory data structure to store key value pairs
	static HashMap<Integer,KeyStore> store = new HashMap<Integer,KeyStore>();
	
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
			KeyStore temp = new KeyStore(key,splitValue[1],Long.parseLong(splitValue[2]));
			if(store.containsKey(key)) {
				store.replace(key, temp);
			}
			else {
				store.put(key, temp);
			}
		}
	}

	public void printStore() {
		for(int key: store.keySet()) {
			System.out.println(store.get(key).getKey() + " " + store.get(key).getValue() + " " + store.get(key).getTimestamp());
		}
	}

	public HashMap<String,Boolean> getConnectedServers() {
		return connectedServers;
	}
	
	public boolean containsServer(String name) {
		return connectedServers.containsKey(name);
	}
	
	public boolean getServerStatus(String name) {
		return connectedServers.get(name);
	}

    //Adds the state of the connected server...true -> connected, false -> not connected
	public void addConnectedServers(String serverName, Boolean b) {
		
		if(connectedServers.containsKey(serverName)) {
			connectedServers.replace(serverName, b);
		}
		else {
			connectedServers.put(serverName, b);
		}
	}
	
	//returns the number of servers connected
	public int getCountConnectedServers() {
		int value = 0;
		for(String name: connectedServers.keySet()) {
			if(connectedServers.get(name) == true) {
				value++;
			}
		}
		
		return value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
}
