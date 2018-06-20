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

/**
* 
* @author  Surendra kumar Koneti
* @since   2017-11-21
*/

public class ServerContext {
	
	private String serverName = null;
	static ServerSocket server;
	static int port = 0;
	private int flag = 0;
	
	//maintains status of the replica servers value, true -> connected and false -> not connected.
	private HashMap<String,Boolean> connectedServers = new HashMap<String,Boolean>();
	private HashMap<String,Boolean> hintServers = new HashMap<String,Boolean>();
	
	//map<servernames, IP> to store all IP addresses of servers.
	static HashMap<String,String> serversIp = new HashMap<String,String>();
	
	//map<serverNames,port> to store all port numbers of servers.
	static HashMap<String,Integer> serversPort = new HashMap<String,Integer>();
	
	//In memory data structure to store key value pairs.
	static HashMap<Integer,KeyValue.KeyValuePair> store = new HashMap<Integer,KeyValue.KeyValuePair>();
	
	//stores a list of put keys for a crashed server.
	public HashMap<String, ArrayList<KeyValue.HintedHandoff> > hintedHandoffMap = new HashMap<String, ArrayList<KeyValue.HintedHandoff>>();
	
	/**
	 * 
	 * @param nameIn name of the serve
	 * @param portIn port number of the server
	 */
	public ServerContext(String nameIn, int portIn) {
		setName(nameIn);
		port = portIn;
	}

	/**
	 * This method reads the server details from conf.txt file.
	 * @param fp util.fileProcessor instance
	 */
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
	
	/**
	 * This method reads the log data after the server is started.
	 * @param fp util.fileProcessor instance 
	 */
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

	/**
	 * This method returns an HashMap that maintains server status. 
	 * @return connectedServers HashMap containing server status 
	 */
	public HashMap<String,Boolean> getConnectedServers() {
		return connectedServers;
	}
	
	/**
	 * 
	 * @param nameIn Server name
	 * @return boolean value, if the server exists or not
	 */
	public synchronized boolean containsServer(String nameIn) {
		return connectedServers.containsKey(nameIn);
	}
	
	/**
	 * 
	 * @param nameIn Server name
	 * @return status of the server, true or false
	 */
	public synchronized boolean getServerStatus(String nameIn) {
		return connectedServers.get(nameIn);
	}

    /**
     * This method adds the state of the connected server, true -> connected, false -> not connected.
     * @param serverNameIn Server name
     * @param statusIn Server status, false or true
     */
	public synchronized void addConnectedServers(String serverNameIn, Boolean statusIn) {
		
		if(connectedServers.containsKey(serverNameIn)) {
			connectedServers.replace(serverNameIn, statusIn);
		}
		else {
			connectedServers.put(serverNameIn, statusIn);
		}
	}
	
	/**
	 * This method returns the number of servers connected.
	 * @return value number of connected servers
	 */
	public synchronized int getCountConnectedServers() {
		int value = 0;
		for(String name: connectedServers.keySet()) {
			if(connectedServers.get(name) == true) {
				value++;
			}
		}		
		return value;
	}

	public synchronized boolean containshintServer(String nameIn) {
		return hintServers.containsKey(nameIn);
	}
	
	public synchronized boolean gethintStatus(String nameIn) {
		return hintServers.get(nameIn);
	}

    //Adds the state of the connected servers...true -> connected, false -> not connected
	public synchronized void addhintServers(String serverNameIn, Boolean statusIn) {
		
		if(hintServers.containsKey(serverNameIn)) {
			hintServers.replace(serverNameIn, statusIn);
		}
		else {
			hintServers.put(serverNameIn, statusIn);
		}
	}
	
	/**
	 * 
	 * @return nameIn Server name
	 */
	public String getName() {
		return serverName;
	}

	/**
	 * 
	 * @param nameIn sets the server name
	 */
	public void setName(String nameIn) {
		this.serverName = nameIn;
	}
	
	/**
	 * This method performs hinted Hand-off mechanism when a previously connected server comes back live.
	 * @param name Server name
	 */
	public void hintedHandoff(String name) {
		
		//loops over the hintedHandoffMap HashMap, matches the server name..if exists, send the key to that server.
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
