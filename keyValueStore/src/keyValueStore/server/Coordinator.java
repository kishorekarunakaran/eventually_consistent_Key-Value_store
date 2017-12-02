package keyValueStore.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import keyValueStore.keyValue.KeyValue;

public class Coordinator implements Runnable{
	
	private Socket clientSocket = null;
	private ServerContext sc = null;
	private KeyValue.KeyValueMessage keyValueMsg = null;
	
	//Map<Id,Consistency> 
	private HashMap<Integer,Integer> consistencyMap = new HashMap<Integer,Integer>();
	
	//Map<Id,response received>
	private HashMap<Integer, Integer> readResponseMap = new HashMap<Integer,Integer>();
	private HashMap<Integer, Integer> writeResponseMap = new HashMap<Integer,Integer>();
	
	//Map<Id, latest key-value pair>
	private HashMap<Integer,ReadRepair> readRepairMap = new HashMap<Integer,ReadRepair>();

	//Constructor
	public Coordinator(Socket in, ServerContext scIn, KeyValue.KeyValueMessage msgIn) {
		clientSocket = in;
		sc = scIn;
		keyValueMsg = msgIn;
		System.out.println("Starting Co-ordinator...");
	}
	
	/**
	 * This function receives message from client, adds current timestamp and forwards to all other replicas
	 * @param incomingMsg
	 */
	private void handleClient(KeyValue.KeyValueMessage incomingMsg) {
		
		KeyValue.KeyValueMessage.Builder keyValueBuilder = null;
		
		//recevied put msg from client
		if(incomingMsg.hasPutKey()) {
			
			KeyValue.Put putMessage = incomingMsg.getPutKey();
			Date date = new Date();
			long time = date.getTime();
			int consistency = putMessage.getConsistency();
	
			KeyValue.Put.Builder putServer = KeyValue.Put.newBuilder();		
			KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
			
			keyStore.setKey(putMessage.getKeyval().getKey());
			keyStore.setValue(putMessage.getKeyval().getValue());				
			keyStore.setTime(time);
						
			putServer.setKeyval(keyStore.build());
			putServer.setConsistency(consistency);
			putServer.setId(putMessage.getId());
			
			consistencyMap.put(putServer.getId(),consistency);
			writeResponseMap.put(putServer.getId(), 0);
			
			keyValueBuilder = KeyValue.KeyValueMessage.newBuilder();
			keyValueBuilder.setConnection(0);
			keyValueBuilder.setPutKey(putServer.build());
			
			sendToServers(keyValueBuilder,consistency);

		}
		
		if(incomingMsg.hasGetKey()) {
			
			KeyValue.Get getMessage = incomingMsg.getGetKey();
			int consistency = getMessage.getConsistency();
			
			KeyValue.Get.Builder getServer = KeyValue.Get.newBuilder();	
			
			getServer.setKey(getMessage.getKey());
			getServer.setConsistency(getMessage.getConsistency());
			getServer.setId(getMessage.getId());
					
			consistencyMap.put(getMessage.getId(), consistency);
			readResponseMap.put(getMessage.getId(), 0);
			
			keyValueBuilder = KeyValue.KeyValueMessage.newBuilder();
			keyValueBuilder.setConnection(0);
			keyValueBuilder.setGetKey(getServer.build());
		
			sendToServers(keyValueBuilder,consistency);

		}
	}

	/**
	 * This function creates new socket connection to all replica servers and forwards message to servers
	 * @param in
	 */
	private void sendToServers(KeyValue.KeyValueMessage.Builder keyIn, int consistency) {
		
		for(String serverName : sc.serversIp.keySet()) {
			try {
				keyIn.setServerName(sc.getName());
				Socket socket = new Socket(sc.serversIp.get(serverName), sc.serversPort.get(serverName));
				OutputStream out = socket.getOutputStream();
				keyIn.build().writeDelimitedTo(out);
				
				//checks if the previous state of the server is false, then performs hintedhandoff.
				if(sc.containsServer(serverName) && sc.getServerStatus(serverName) == false) {
					sc.hintedHandoff(serverName);
				}
				
				sc.addConnectedServers(serverName, true);
				out.flush();
			
				//Thread to listen response from the requested server
				new Thread(new Runnable(){
					
					public void run() {
						try {
							String server_name = serverName;
							InputStream in = socket.getInputStream();
							KeyValue.KeyValueMessage responseMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
							
							handleServer(server_name,responseMsg);
							
							in.close();
							socket.close();
							
						}catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}).start();
				
			} catch(ConnectException e) {
				//if a server not reachable, set its status to false
				sc.addConnectedServers(serverName, false);
				//add the key to the hashmap to send it later
				if(keyIn.hasPutKey()) {
					ArrayList<KeyValue.HintedHandoff> ls = null;
					if(sc.hintedHandoffMap.containsKey(serverName)) {
						ls = sc.hintedHandoffMap.get(serverName);
					}
					else {
						ls = new ArrayList<KeyValue.HintedHandoff>();
					}
					
					KeyValue.HintedHandoff.Builder hh = KeyValue.HintedHandoff.newBuilder();
					hh.setKeyval(keyIn.getPutKey().getKeyval());
					hh.setId(keyIn.getPutKey().getId());
					
					ls.add(hh.build());
					sc.hintedHandoffMap.put(serverName, ls);
				}				
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
		}
	}	
	
	
	/**
	 * This function updates the consistency map and responds to client accordingly
	 * @param server name, responseMsg
	 * @throws IOException
	 */
	private synchronized void handleServer(String serverName, KeyValue.KeyValueMessage responseMsg) throws IOException {
		
		if(responseMsg.hasWriteResponse()) {
			KeyValue.WriteResponse wr = responseMsg.getWriteResponse();
			int id = wr.getId();
			if(wr.getWriteReply()) {
				
				int cVal = writeResponseMap.get(id);
				//Calculate total number of response received 
				writeResponseMap.replace(id,cVal+1);
			}
			
			//IF total no. of response received is equal to consistency level, return to client
			if(consistencyMap.get(id) == writeResponseMap.get(id)) {
				
				System.out.println("Sending write response to client: key= " + wr.getKey() + " " + wr.getWriteReply());
				consistencyMap.replace(id, -1);
				
				//send response to client
				KeyValue.KeyValueMessage.Builder responseClient = KeyValue.KeyValueMessage.newBuilder();
				responseClient.setWriteResponse(wr);
				
				OutputStream out = clientSocket.getOutputStream();
				responseClient.build().writeDelimitedTo(out);
				out.flush();
			}
		}
		
		if(responseMsg.hasReadResponse()) {
			
			KeyValue.ReadResponse rr = responseMsg.getReadResponse();
			int id = rr.getId();
			
			if(rr.getReadStatus()) {
				int key = rr.getKeyval().getKey();
				String value = rr.getKeyval().getValue();
				long time = rr.getKeyval().getTime();
				
				//first response received....
				if(!readRepairMap.containsKey(id)) {			
					ReadRepair r = new ReadRepair(id, key, value, time);
					r.addServers(serverName, true);
					
					readRepairMap.put(id, r);
				}
				readRepairMap.get(id).setReadStatus(true);
				//Checks if the received message has a timestamp greater than the one in readRepairMap for the id; if true replaces with the latest timestamp;
				if(time > readRepairMap.get(id).getTimestamp()) {
					readRepairMap.get(id).setId(id);
					readRepairMap.get(id).setKey(key);
					readRepairMap.get(id).setValue(value);
					readRepairMap.get(id).setTimestamp(time);
					readRepairMap.get(id).updateServers();
					readRepairMap.get(id).setReadRepairStatus(true);
					readRepairMap.get(id).addServers(serverName, true);
				}
				
				//if received message has a timestamp lesser than the one in readRepairMap, then the corresponding server has to be sent the updated key-value pair
				if(time < readRepairMap.get(id).getTimestamp()) {
					readRepairMap.get(id).addServers(serverName, false);
					readRepairMap.get(id).setReadRepairStatus(true);
				}
				
			}
			else {
				int key = rr.getKeyval().getKey();
				
				if(!readRepairMap.containsKey(id)) {
					ReadRepair r = new ReadRepair(id, key, null, 0);
					r.addServers(serverName, false);
					r.setReadStatus(false);
					readRepairMap.put(id,r);
				}
				
				readRepairMap.get(id).addServers(serverName, false);
				
			}
					
			int cVal = readResponseMap.get(id);
			readResponseMap.replace(id, cVal+1);
						
			//IF total no. of response received is equal to consistency level, return to client
			if(consistencyMap.get(id) == readResponseMap.get(id)){
				
				System.out.println("Sending read response to client: key = " + readRepairMap.get(id).getKey());
				consistencyMap.replace(id, -1);
				
				//send response to client
				KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
				KeyValue.ReadResponse.Builder readResponse = KeyValue.ReadResponse.newBuilder();				
				KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
				
				if(readRepairMap.get(id).getReadStatus()) {
					keyStore.setKey(readRepairMap.get(id).getKey());
					keyStore.setValue(readRepairMap.get(id).getValue());		
					keyStore.setTime(readRepairMap.get(id).getTimestamp());
					
				}
				else {
					keyStore.setKey(readRepairMap.get(id).getKey());
				}
				
				readResponse.setKeyval(keyStore.build());
				readResponse.setId(readRepairMap.get(id).getId());
				readResponse.setReadStatus(readRepairMap.get(id).getReadStatus());
				
				keyMessage.setReadResponse(readResponse.build());
				
				OutputStream out = clientSocket.getOutputStream();
				keyMessage.build().writeDelimitedTo(out);
				out.flush();
			}
			
			//All the responses received.. update inconsistant data in other servers if any exist
			if(readResponseMap.get(id) == sc.getCountConnectedServers()) {
				
				//check if readRepair has to be done or not
				if(readRepairMap.get(id).getReadRepairStatus() == true) {
					
					//list of server names that needs to be updated
					HashMap<String,Boolean> list = readRepairMap.get(id).getServers();
					
					KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
				    KeyValue.ReadRepair.Builder readRepairMsg = KeyValue.ReadRepair.newBuilder();					
					KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
					
					keyStore.setKey(readRepairMap.get(id).getKey());
					keyStore.setValue(readRepairMap.get(id).getValue());		
					keyStore.setTime(readRepairMap.get(id).getTimestamp());
					
					readRepairMsg.setKeyval(keyStore.build());
				    readRepairMsg.setId(id);
				    
					keyMessage.setReadRepair(readRepairMsg.build());
					
					for(String name : list.keySet()) {
						if(list.get(name) == false) {
							try {
								
								System.out.println("Sending readRepair message to " + name + "  Key:  " + readRepairMap.get(id).getKey());
								Socket sock = new Socket(sc.serversIp.get(name), sc.serversPort.get(name));
								OutputStream out = sock.getOutputStream();
								keyMessage.build().writeDelimitedTo(out);
								out.flush();
								out.close();
								sock.close();
								
							} catch(ConnectException e) {
								
								sc.addConnectedServers(name, false);
								ArrayList<KeyValue.HintedHandoff> ls = null;
								
								if(sc.hintedHandoffMap.containsKey(serverName)) {
									ls = sc.hintedHandoffMap.get(serverName);
								}
								else {
									ls = new ArrayList<KeyValue.HintedHandoff>();
								}
								
								KeyValue.HintedHandoff.Builder hh = KeyValue.HintedHandoff.newBuilder();
								hh.setKeyval(keyMessage.getReadRepair().getKeyval());
								hh.setId(keyMessage.getReadRepair().getId());
								
								ls.add(hh.build());
								sc.hintedHandoffMap.put(serverName, ls);
								
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
	}
	
	@Override
	public void run() {
		
		//Processing the first keyValue message received from the client
		if(keyValueMsg != null) {
			handleClient(keyValueMsg);
		}
		
		while(true) {
			try {
				InputStream in = clientSocket.getInputStream();
				KeyValue.KeyValueMessage incomingMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
				if(incomingMsg != null) {
					handleClient(incomingMsg);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}						
		}
	}
}
