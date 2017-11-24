import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;

public class Coordinator implements Runnable{
	
	private Socket clientSocket = null;
	private ServerContext sc = null;
	private KeyValue.KeyValueMessage keyValueMsg = null;
	
	//Map<Key,Consistency> 
	private HashMap<Integer,Integer> consistencyMap = new HashMap<Integer,Integer>();
	
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
			
			consistencyMap.put(putserver.getKey(),putserver.getConsistency());
			
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
			
			consistencyMap.put(getMessage.getKey(), getMessage.getConsistency());
			
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
				out.flush();
				
				//Thread to listen response from the requested server
				new Thread(new Runnable(){
					
					public void run() {
						try {
							InputStream re = socket.getInputStream();
							KeyValue.KeyValueMessage responseMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(re);
							
							updateConsistencyMap(responseMsg);
							
						}catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}).start();
				
			} catch (UnknownHostException e) {
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
	private synchronized void updateConsistencyMap(KeyValue.KeyValueMessage responseMsg) throws IOException {
		
		if(responseMsg.hasWriteResponse()) {
			KeyValue.WriteResponse wr = responseMsg.getWriteResponse();
			int key = wr.getKey();
			if(wr.getWriteReply() && ( consistencyMap.get(key) > 0 )) {
				int val = consistencyMap.get(key);
				
				//Calculate total number of response received 
				val = val - 1;
				consistencyMap.replace(key,val);
			}
			
			//IF total no. of response received is equal to consistency level, return to client
			if(consistencyMap.get(key) == 0) {
				System.out.println("Response received from all client(equal to consistency level) for key= " + key + " " + wr.getWriteReply());
				consistencyMap.replace(key, -1);
				
				//send response to client
				KeyValue.KeyValueMessage.Builder res = KeyValue.KeyValueMessage.newBuilder();
				res.setWriteResponse(wr);
				
				OutputStream out = clientSocket.getOutputStream();
				res.build().writeDelimitedTo(out);
			}
		}
		
		if(responseMsg.hasReadResponse()) {
			KeyValue.ReadResponse readResp = responseMsg.getReadResponse();
			int key = readResp.getKey();
			
			if(( consistencyMap.get(key) > 0 )) {
				int val = consistencyMap.get(key);
				
				//Calculate total number of response received 
				val = val - 1;
				consistencyMap.replace(key,val);
			}
			
			//IF total no. of response received is equal to consistency level, return to client
			if(consistencyMap.get(key) == 0) {
				System.out.println("Response received from all client(equal to consistency level) for key= " + key);
				consistencyMap.replace(key, -1);
				
				//send response to client
				KeyValue.KeyValueMessage.Builder res = KeyValue.KeyValueMessage.newBuilder();
				res.setReadResponse(readResp);
				
				OutputStream out = clientSocket.getOutputStream();
				res.build().writeDelimitedTo(out);
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
