package keyValueStore.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;

import keyValueStore.keyValue.KeyValue;

public class Coordinator implements Runnable{
	
	private Socket clientSocket = null;
	private ServerContext sc = null;
	private KeyValue.KeyValueMessage keyValueMsg = null;
	
	//Map<Id,Consistency> 
	private HashMap<Integer,Integer> consistencyMap = new HashMap<Integer,Integer>();
	
	//Map<Id,read response received>
	private HashMap<Integer, Integer> readResponseMap = new HashMap<Integer,Integer>();
	
	//Map<Id, latest key-value pair>
	private HashMap<Integer,ReadRepair> readRepairMap = new HashMap<Integer,ReadRepair>();
	
	public Coordinator(Socket in, ServerContext scIn, KeyValue.KeyValueMessage msgIn) {
		clientSocket = in;
		sc = scIn;
		keyValueMsg = msgIn;
	}
	
	/**
	 * This function receives message from client, adds current timestamp and forwards to all other replicas
	 * @param incomingMsg
	 */
	private void handleClient(KeyValue.KeyValueMessage incomingMsg) {
		
		KeyValue.KeyValueMessage.Builder keyValueBuilder = null;
		
		//recevied put msg from client
		if(incomingMsg.hasPutKey()) {
			KeyValue.Put put = incomingMsg.getPutKey();
			
			KeyValue.Put.Builder putserver = KeyValue.Put.newBuilder();
			putserver.setKey(put.getKey());
			putserver.setValue(put.getValue());
			putserver.setConsistency(put.getConsistency());
			
			Date date = new Date();
			long time = date.getTime();
			putserver.setTime(time);
			putserver.setId(put.getId());
			
			consistencyMap.put(putserver.getId(),putserver.getConsistency());
			
			keyValueBuilder = KeyValue.KeyValueMessage.newBuilder();
			keyValueBuilder.setConnection(0);
			keyValueBuilder.setPutKey(putserver).build();
			
			sendToServers(keyValueBuilder);
			
		}
		
		if(incomingMsg.hasGetKey()) {
			KeyValue.Get getMessage = incomingMsg.getGetKey();
			
			KeyValue.Get.Builder getMsgBuilder = KeyValue.Get.newBuilder();
			getMsgBuilder.setKey(getMessage.getKey());
			getMsgBuilder.setConsistency(getMessage.getConsistency());
			getMsgBuilder.setId(getMessage.getId());
			
			consistencyMap.put(getMessage.getId(), getMessage.getConsistency());
			readResponseMap.put(getMessage.getId(), 0);
			
			keyValueBuilder = KeyValue.KeyValueMessage.newBuilder();
			keyValueBuilder.setConnection(0);
			keyValueBuilder.setGetKey(getMsgBuilder);
		
			sendToServers(keyValueBuilder);
		}
	}

	/**
	 * This function creates new socket connection to all replica servers and forwards message to servers
	 * @param in
	 */
	private void sendToServers(KeyValue.KeyValueMessage.Builder in) {
		
		for(String serverName : sc.serversIp.keySet()) {
			try {
				
				Socket socket = new Socket(sc.serversIp.get(serverName), sc.serversPort.get(serverName));
				OutputStream out = socket.getOutputStream();
				in.build().writeDelimitedTo(out);
				sc.addConnectedServers(serverName, true);
				out.flush();
				
				//Thread to listen response from the requested server
				new Thread(new Runnable(){
					
					public void run() {
						try {
							String server_name = serverName;
							InputStream re = socket.getInputStream();
							KeyValue.KeyValueMessage responseMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(re);
							
							updateConsistencyMap(server_name,responseMsg);
							re.close();
							socket.close();
							
						}catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}).start();
				
			}catch(ConnectException e) {
				sc.addConnectedServers(serverName, false);
			}
			catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
	}	

	/**
	 * This function updates the consistency map and responds to client accordingly
	 * @param responseMsg
	 * @throws IOException
	 */
	private synchronized void updateConsistencyMap(String serverName, KeyValue.KeyValueMessage responseMsg) throws IOException {
		
		if(responseMsg.hasWriteResponse()) {
			KeyValue.WriteResponse wr = responseMsg.getWriteResponse();
			int id = wr.getId();
			if(wr.getWriteReply() && ( consistencyMap.get(id) > 0 )) {
				int val = consistencyMap.get(id);
				
				//Calculate total number of response received 
				val = val - 1;
				consistencyMap.replace(id,val);
			}
			
			//IF total no. of response received is equal to consistency level, return to client
			if(consistencyMap.get(id) == 0) {
				System.out.println("Response received from all client(equal to consistency level) for key= " + id + " " + wr.getWriteReply());
				consistencyMap.replace(id, -1);
				
				//send response to client
				KeyValue.KeyValueMessage.Builder res = KeyValue.KeyValueMessage.newBuilder();
				res.setWriteResponse(wr);
				
				OutputStream out = clientSocket.getOutputStream();
				res.build().writeDelimitedTo(out);
			}
		}
		
		if(responseMsg.hasReadResponse()) {
			KeyValue.ReadResponse readResp = responseMsg.getReadResponse();
			int id = readResp.getId();
			
			//First response received..add's the id and the key-value messages to readRepairMap
			if(!readRepairMap.containsKey(id)) {			
				ReadRepair r = new ReadRepair(id, readResp.getKey(), readResp.getValue(), readResp.getTime());
				r.addServers(serverName, true);
				readRepairMap.put(id, r);
			}
			
			int val = readResponseMap.get(id);
			readResponseMap.replace(id, val+1);
			
			long time = readResp.getTime();
			
			//Checks if the received message has a timestamp greater than the one in readRepairMap for the id; if true replaces with the latest timestamp;
			if(time > readRepairMap.get(id).getTimestamp()) {
				readRepairMap.get(id).setId(id);
				readRepairMap.get(id).setKey(readResp.getKey());
				readRepairMap.get(id).setValue(readResp.getValue());
				readRepairMap.get(id).setTimestamp(readResp.getTime());
				readRepairMap.get(id).updateServers();
				readRepairMap.get(id).setReadRepairStatus(true);
				readRepairMap.get(id).addServers(serverName, true);
			}
			
			//if received message has a timestamp lesser than the one in readRepairMap, then the corresponding server has to be sent the updated key-value pair
			if(time < readRepairMap.get(id).getTimestamp()) {
				readRepairMap.get(id).addServers(serverName, false);
				readRepairMap.get(id).setReadRepairStatus(true);
			}
			
			//IF total no. of response received is equal to consistency level, return to client
			if(consistencyMap.get(id) == readResponseMap.get(id)) {
				System.out.println("Response received from clients(equal to consistency level) for key= " + id);
				consistencyMap.replace(id, -1);
				
				//send response to client
				KeyValue.KeyValueMessage.Builder res = KeyValue.KeyValueMessage.newBuilder();
				KeyValue.ReadResponse.Builder readResponse = KeyValue.ReadResponse.newBuilder();
				readResponse.setKey(readRepairMap.get(id).getKey());
				readResponse.setValue(readRepairMap.get(id).getValue());
				readResponse.setTime(readRepairMap.get(id).getTimestamp());
				readResponse.setId(readRepairMap.get(id).getId());
				
				res.setReadResponse(readResponse);
				
				OutputStream out = clientSocket.getOutputStream();
				res.build().writeDelimitedTo(out);
				out.flush();
			}
			
			//All the responses received.. update inconsistant data in other servers if any exist
			if(readResponseMap.get(id) == sc.getCountConnectedServers()) {
				
				//check if readRepair has to be done or not
				if(readRepairMap.get(id).getReadRepairStatus() == true) {
					
					//list of server names that needs to be updated
					HashMap<String,Boolean> list = readRepairMap.get(id).getServers();
					
					KeyValue.KeyValueMessage.Builder keymessage = KeyValue.KeyValueMessage.newBuilder();
					KeyValue.Put.Builder putMethod = KeyValue.Put.newBuilder();
					putMethod.setKey(readRepairMap.get(id).getKey());
					putMethod.setValue(readRepairMap.get(id).getValue());
					putMethod.setTime(readRepairMap.get(id).getTimestamp());
					//notify it is an update message to servers..set readRepair field to 1
					putMethod.setReadRepair(1);
					keymessage.setPutKey(putMethod.build());
					
					for(String name : list.keySet()) {
						if(list.get(name) == false) {
							try {
								System.out.println("Sending readRepair message to " + name + " " + putMethod.getKey());
								Socket sock = new Socket(sc.serversIp.get(name), sc.serversPort.get(name));
								OutputStream out = sock.getOutputStream();
								keymessage.build().writeDelimitedTo(out);
								out.flush();
								out.close();
								sock.close();
							}catch(ConnectException e) {
								sc.addConnectedServers(name, false);
							}catch (UnknownHostException e) {
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
