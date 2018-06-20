package keyValueStore.server;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.InputStream;
import java.io.OutputStream;

import keyValueStore.keyValue.KeyValue;
import keyValueStore.util.FileProcessor;
import keyValueStore.util.writeLog;

/**
* 
* @author  Surendra kumar Koneti
* @since   2017-11-21
*/

public class ReplicaServer{
	
	public static void main(String[] args){
		
		if(args.length != 4){
			System.out.println("Usage: ./server.sh <server name> <port> <config file> <read repair:1 or hinted handoff:0>\n");
			System.exit(0);
		}
				
		ServerContext sc = new ServerContext(args[0],Integer.parseInt(args[1]));
		FileProcessor fp = new FileProcessor(args[2]);
		
		//user input, to use read repair or hinted hand-off.
		int flag = Integer.parseInt(args[3]);
		
		sc.readFile(fp);
		fp.close();
		sc.setFlag(flag);
		
		//log file, path = /log/servername.log.
		String path = "log/" + sc.getName() +".log";			
		FileProcessor readLog = new FileProcessor(path);
		
		//checks if the log file exists or not.. true -> reads log file, false -> file doesn't exist.
		if(readLog.isReadable()) {
		
			sc.readLog(readLog);
			readLog.close();
			sc.printStore();
		}
		
		writeLog wrlog = new writeLog(path);
		
		try {
			ServerContext.server = new ServerSocket(ServerContext.port);
		}
		catch(IOException i) {
			System.out.println(i);
		}
	
		try {
			System.out.println("Listening on " + InetAddress.getLocalHost().getHostAddress() +" " + + ServerContext.port);
		} 
		catch(UnknownHostException e1) {
			e1.printStackTrace();
		}
		
		new Thread(new Runnable() {

			@Override
			public void run() {
				while(true) {
					KeyValue.KeyValueMessage.Builder km = KeyValue.KeyValueMessage.newBuilder();
					km.setConnection(0);
					km.setServerName(sc.getName());
					
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
			//		System.out.println("pinging...");
					for(String serverName : ServerContext.serversIp.keySet()) {
						try {
							Socket socket = new Socket(ServerContext.serversIp.get(serverName), ServerContext.serversPort.get(serverName));
							OutputStream out = socket.getOutputStream();
							km.build().writeDelimitedTo(out);
							sc.addConnectedServers(serverName, true);
							out.flush();
							out.close();
							socket.close();
												
						} catch(ConnectException e) {
					//		System.out.println(serverName + "not available");
							sc.addConnectedServers(serverName, false);
						} catch(UnknownHostException e) {
							e.printStackTrace();
						} catch(IOException e) {
							sc.addConnectedServers(serverName, false);
							e.printStackTrace();
						}
					
					}
				//	System.out.println("Connected Servers " + sc.getCountConnectedServers());
				}
			}
		}).start();
		
		Socket request = null;
		
		while(true) {
			try {
				
				request = ServerContext.server.accept();
				
				InputStream in = request.getInputStream();
				KeyValue.KeyValueMessage keyValueMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
				
				//Connection -> 1, message from client.
				if(keyValueMsg.getConnection() == 1) {
					
					System.out.println("Received message from Client...");
					Thread coordinatorThread = new Thread(new Coordinator(request, sc, keyValueMsg));
					coordinatorThread.start();
					
				}
				
				//Connection -> 0, message from replica servers.
				if(keyValueMsg.getConnection() == 0){
					
				//	System.out.println("Received message from Server...");
					
					String receiveServer = keyValueMsg.getServerName();
					
				//	sc.addConnectedServers(receiveServer, true);
					
					KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
					OutputStream out = null;
					
					if(keyValueMsg.hasPutKey()) {
						
						if(sc.getFlag() == 0) {
							hintedhandoff(receiveServer, sc);
						}
						KeyValue.Put put = keyValueMsg.getPutKey();
						KeyValue.KeyValuePair keyStore = put.getKeyval();

						String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
						wrlog.writeToFile(writeAheadLog);						
						ServerContext.store.put(keyStore.getKey(), keyStore);							
						System.out.println("Message written to keyStore..!!\n");
						sc.printStore();
							
						KeyValue.WriteResponse.Builder wr = KeyValue.WriteResponse.newBuilder();
						wr.setKey(keyStore.getKey());
						wr.setId(put.getId());
						wr.setWriteReply(true);
						
						//reply back to Coordinator after message written to KeyStore.
						out = request.getOutputStream();
						keyMessage.setWriteResponse(wr.build());
						keyMessage.build().writeDelimitedTo(out);
						out.flush();
						out.close();
						
					}
					
					//Read Repair message, update the KeyValue store.	
					if(keyValueMsg.hasReadRepair()) {
						
						KeyValue.ReadRepair rr = keyValueMsg.getReadRepair();
						KeyValue.KeyValuePair keyStore = rr.getKeyval();
						int key = keyStore.getKey();
						
						String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
						wrlog.writeToFile(writeAheadLog);
						ServerContext.store.put(key, keyStore);						
						System.out.println("Read Repair done...");
						sc.printStore();
					
					}
					
					//Hinted Hand-off message, checks the TimeStamp and updates the KeyValue store.
					if(keyValueMsg.hasHintedHandoff()) {
						
						KeyValue.HintedHandoff hh = keyValueMsg.getHintedHandoff();
						KeyValue.KeyValuePair keyStore = hh.getKeyval();
						int key = keyStore.getKey();
						
						
						if(ServerContext.store.containsKey(key) && keyStore.getTime() >= ServerContext.store.get(key).getTime()) {
							String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
							wrlog.writeToFile(writeAheadLog);
							ServerContext.store.replace(key, keyStore);
							System.out.println("Hinted Handoff done...");
							sc.printStore();
						}
						else {
							String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
							wrlog.writeToFile(writeAheadLog);
							ServerContext.store.put(key, keyStore);
							System.out.println("Hinted Handoff done...");
							sc.printStore();
						}					
					}
					
					if(keyValueMsg.hasGetKey()) {
						
						if(sc.getFlag() == 0) {
							hintedhandoff(receiveServer, sc);
						}
						
						int key = keyValueMsg.getGetKey().getKey();
						KeyValue.KeyValuePair keyStore = null;
						KeyValue.ReadResponse.Builder readResp = KeyValue.ReadResponse.newBuilder();
						
						if(ServerContext.store.containsKey(key)) {
							keyStore = ServerContext.store.get(key);
							readResp.setKeyval(keyStore);
							readResp.setId(keyValueMsg.getGetKey().getId());
							readResp.setReadStatus(true);
							
						}					
						else {
							KeyValue.KeyValuePair.Builder ks = KeyValue.KeyValuePair.newBuilder();
							ks.setKey(key);
							readResp.setKeyval(ks);
							readResp.setId(keyValueMsg.getGetKey().getId());
							readResp.setReadStatus(false);
						}
						
						System.out.println("Read response sent...");
						//reply back to Coordinator.
						out = request.getOutputStream();
						keyMessage.setReadResponse(readResp);
						keyMessage.build().writeDelimitedTo(out);
						out.flush();
						out.close();
						
					}	
			
					in.close();
					request.close();	
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * This method performs Hinted Hand-off when this server comes to know that a previously connected server is back live.
	 * @param serverName Name of the Server
	 * @param sc ServerContext instance 
	 */
	private static void hintedhandoff(String serverName, ServerContext sc) {
		
		String receiveServer = serverName;
		if(sc.containshintServer(receiveServer) && sc.gethintStatus(receiveServer) == false) {
			
			if(sc.hintedHandoffMap.containsKey(receiveServer)) {
				System.out.println("Performing hinted handoff...");
				sc.addhintServers(receiveServer, true);
				sc.hintedHandoff(receiveServer);
			}
		}
		
	}
}
