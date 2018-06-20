package keyValueStore.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
* 
* @author  Surendra kumar Koneti
* @since   2017-11-21
*/

import keyValueStore.keyValue.KeyValue;

public class Coordinator implements Runnable{
	
	private Socket clientSocket = null;
	private ServerContext sc = null;
	private KeyValue.KeyValueMessage keyValueMsg = null;
	
	//HashMap<Id,Consistency>
	private HashMap<Integer,Integer> consistencyMap = new HashMap<Integer,Integer>();
	
	//HashMap<Id,response received>
	private HashMap<Integer, Integer> readResponseMap = new HashMap<Integer,Integer>();
	private HashMap<Integer, Integer> repliesMap = new HashMap<Integer,Integer>();
	private HashMap<Integer, Integer> writeResponseMap = new HashMap<Integer,Integer>();
	
	//HashMap<Id, latest key-value pair>
	private HashMap<Integer,ReadRepair> readRepairMap = new HashMap<Integer,ReadRepair>();

	/**
	 * 
	 * @param in Socket instance of the server
	 * @param scIn ServerContext instance of the server
	 * @param msgIn First message sent to the server
	 */
	public Coordinator(Socket in, ServerContext scIn, KeyValue.KeyValueMessage msgIn) {
		clientSocket = in;
		sc = scIn;
		keyValueMsg = msgIn;
		System.out.println("Starting Co-ordinator...");
	}
	
	/**
	 * This function receives message from client, adds current timestamp and forwards to all other replicas
	 * @param incomingMsg handles the message sent to the server
	 */
	private void handleClient(KeyValue.KeyValueMessage incomingMsg) {
		
		KeyValue.KeyValueMessage.Builder keyValueBuilder = null;
		
		//received put message from client
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
			
		//	System.out.println("Servers connected " + sc.getCountConnectedServers());
			if(sc.getCountConnectedServers() < consistency) {
				System.out.println("Exception message");
				KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
				KeyValue.Exception.Builder excep = KeyValue.Exception.newBuilder();
				excep.setKey(putServer.getKeyval().getKey());
				excep.setMethod("PUT");
				excep.setExceptionMessage("Number of online servers is less than the consistency level");
				keyMessage.setException(excep.build());
				try {
					OutputStream out = clientSocket.getOutputStream();
					keyMessage.build().writeDelimitedTo(out);
					out.flush();
					
				} catch(IOException i) {
					System.out.println("Client not reachable...");
					i.printStackTrace();
				}
				
			}else {
				sendToServers(keyValueBuilder);
			}
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
			repliesMap.put(getMessage.getId(), 0);
			
			keyValueBuilder = KeyValue.KeyValueMessage.newBuilder();
			keyValueBuilder.setConnection(0);
			keyValueBuilder.setGetKey(getServer.build());
		
	//		System.out.println("Servers connected " + sc.getCountConnectedServers());
			
			/* Checks if the number of servers connected is less than consistency level, if true sends exception message to client
			*  or sends the key value message to other servers
			*/
			if(sc.getCountConnectedServers() < consistency) {
				System.out.println("Exception message");
				KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
				KeyValue.Exception.Builder excep = KeyValue.Exception.newBuilder();
				excep.setKey(getServer.getKey());
				excep.setMethod("GET");
				excep.setExceptionMessage("Number of online servers is less than the consistency level");
				keyMessage.setException(excep.build());
				try {
					OutputStream out = clientSocket.getOutputStream();
					keyMessage.build().writeDelimitedTo(out);
					out.flush();
					
				} catch(IOException i) {
					System.out.println("Client not reachable...");
					i.printStackTrace();
				}
				
			}else {
				sendToServers(keyValueBuilder);
			}
		}
	
	}

	/**
	 * This function creates new socket connection to all replica servers and forwards message to servers
	 * @param keyIn KeyValue message to send to other servers 
	 */
	private void sendToServers(KeyValue.KeyValueMessage.Builder keyIn) {
		
		for(String serverName : ServerContext.serversIp.keySet()) {
			try {
				keyIn.setServerName(sc.getName());
				Socket socket = new Socket(ServerContext.serversIp.get(serverName), ServerContext.serversPort.get(serverName));
				OutputStream out = socket.getOutputStream();
				keyIn.build().writeDelimitedTo(out);
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
				System.out.println(serverName + "not reachable");
				
				sc.addhintServers(serverName, false);
				
				//add the key to the HashMap to send it later, Hinted Hand-off
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
	 * @param serverName Server name 
	 * @param responseMsg the response message that is sent to the client
 	 * @throws IOException if client is not connected 
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
				
				try {
					OutputStream out = clientSocket.getOutputStream();
					responseClient.build().writeDelimitedTo(out);
					out.flush();
					
				} catch(IOException i) {
					System.out.println("Client not reachable...");
					//i.printStackTrace();
				}
			}
		}
		
		if(responseMsg.hasReadResponse()) {
			//System.out.println("Received read response from " + serverName);
			KeyValue.ReadResponse rr = responseMsg.getReadResponse();
			int id = rr.getId();
			
			int replies = repliesMap.get(id);
			repliesMap.replace(id, replies+1);
			
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
				readRepairMap.get(id).serversTimestamp.put(serverName, time);
				
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
					//System.out.println("Read Repair should be performed on  " + serverName);
					readRepairMap.get(id).addServers(serverName, false);
					readRepairMap.get(id).setReadRepairStatus(true);
				}
				
				int cVal = readResponseMap.get(id);
				readResponseMap.replace(id, cVal+1);
				
				if(readResponseMap.get(id) == consistencyMap.get(id)) {
					consistencyMap.replace(id, -1);
					KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
					KeyValue.ReadResponse.Builder readResponse = KeyValue.ReadResponse.newBuilder();				
					KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
				
					keyStore.setKey(readRepairMap.get(id).getKey());
					keyStore.setValue(readRepairMap.get(id).getValue());		
					keyStore.setTime(readRepairMap.get(id).getTimestamp());
					readResponse.setKeyval(keyStore.build());
					readResponse.setId(readRepairMap.get(id).getId());
					readResponse.setReadStatus(readRepairMap.get(id).getReadStatus());
					
					keyMessage.setReadResponse(readResponse.build());
					try {
						OutputStream out = clientSocket.getOutputStream();
						keyMessage.build().writeDelimitedTo(out);
						out.flush();
					} catch(IOException i) {
						System.out.println("Client not reachable...");
						//i.printStackTrace();
					}			
				}
				
			}
			//return value null for the readStatus, means it does not have value and read repair has to be performed.
			else {
				int key = rr.getKeyval().getKey();
				
				if(!readRepairMap.containsKey(id)) {
					ReadRepair r = new ReadRepair(id, key, null, 0);
					r.addServers(serverName, false);
					readRepairMap.put(id,r);			
				}
				
				//System.out.println("Read Repair should be performed on(empty response) " + serverName);
				readRepairMap.get(id).addServers(serverName, false);
				
			}
											
			//all the responses received.. update inconsistent data in other servers if any exist.
			
			//System.out.println("-->" + repliesMap.get(id) + " " + sc.getCountConnectedServers());
			if(repliesMap.get(id) == sc.getCountConnectedServers()) {
				if(consistencyMap.get(id) != -1 && readRepairMap.get(id).checkConsistency(consistencyMap.get(id)) == false) {
					KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
					KeyValue.Exception.Builder excep = KeyValue.Exception.newBuilder();
					excep.setKey(readRepairMap.get(id).getKey());
					excep.setMethod("GET");
					excep.setExceptionMessage("Consistency not satisfied");
					keyMessage.setException(excep.build());
					try {
						OutputStream out = clientSocket.getOutputStream();
						keyMessage.build().writeDelimitedTo(out);
						out.flush();
						
					} catch(IOException i) {
						System.out.println("Client not reachable...");
						i.printStackTrace();
					}
					
				}
				else if(readRepairMap.get(id).getReadStatus() == false){
				  KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
					KeyValue.ReadResponse.Builder readResponse = KeyValue.ReadResponse.newBuilder();				
					KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
				
					keyStore.setKey(readRepairMap.get(id).getKey());
					readResponse.setKeyval(keyStore.build());
					readResponse.setId(readRepairMap.get(id).getId());
					readResponse.setReadStatus(readRepairMap.get(id).getReadStatus());
					
					keyMessage.setReadResponse(readResponse.build());
					try {
						OutputStream out = clientSocket.getOutputStream();
						keyMessage.build().writeDelimitedTo(out);
						out.flush();
					} catch(IOException i) {
						System.out.println("Client not reachable...");
						//i.printStackTrace();
					}				
			  }
				
			  if(sc.getFlag() == 1) {
				startReadRepairInBackground(serverName, id);
			  }
			}
		}
	}
	
	/**
	 * This function starts read repair in background if any inconsistent data
	 * @param serverName  Server Name
	 * @param id  the id of the key value message for which read repair is performed
	 */
	private void startReadRepairInBackground(String serverName, int id) {
	
			//System.out.println("status " + readRepairMap.get(id).getReadRepairStatus());
			//check if readRepair has to be done or not
			if(readRepairMap.get(id).getReadStatus() == true) {
				
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
							Socket sock = new Socket(ServerContext.serversIp.get(name), ServerContext.serversPort.get(name));
							OutputStream out = sock.getOutputStream();
							keyMessage.build().writeDelimitedTo(out);
							out.flush();
							out.close();
							sock.close();
							
						} catch(ConnectException e) {
							
							sc.addhintServers(name, false);
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
	
	@Override
	public void run() {
		
		//Processes the first keyValue message received from the client
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
