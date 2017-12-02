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

public class ReplicaServer{
	
	public static void main(String[] args){
		
		if(args.length != 3){
			System.out.println("Usage: ./server.sh <server name> <port> <config file>\n");
			System.exit(0);
		}
				
		ServerContext sc = new ServerContext(args[0],Integer.parseInt(args[1]));
		FileProcessor fp = new FileProcessor(args[2]);
		sc.readFile(fp);
		fp.close();
		
		//log file, path = /log/servername.log
		String path = "log/" + sc.getName() +".log";			
		FileProcessor readLog = new FileProcessor(path);
		
		//checks if the log file exists or not.. true -> reads log file, false -> file doesn't exist
		if(readLog.isReadable()) {
		
			sc.readLog(readLog);
			readLog.close();
			sc.printStore();
		}
		
		//wrireLog class, writes to log..
		writeLog wrlog = new writeLog(path);
		
		try {
			sc.server = new ServerSocket(sc.port);
		}
		catch(IOException i) {
			System.out.println(i);
		}
	
		try {
			System.out.println("Listening on " + InetAddress.getLocalHost().getHostAddress() +" " + + sc.port);
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
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
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			//		System.out.println("pinging...");
					for(String serverName : sc.serversIp.keySet()) {
						try {
							Socket socket = new Socket(sc.serversIp.get(serverName), sc.serversPort.get(serverName));
							OutputStream out = socket.getOutputStream();
							km.build().writeDelimitedTo(out);
							sc.addConnectedServers(serverName, true);
							out.flush();
							out.close();
							socket.close();
												
						} catch(ConnectException e) {
					//		System.out.println(serverName + "not available");
							sc.addConnectedServers(serverName, false);
						} catch (UnknownHostException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							sc.addConnectedServers(serverName, false);
							// TODO Auto-generated catch block
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
				
				request = sc.server.accept();
				
				InputStream in = request.getInputStream();
				KeyValue.KeyValueMessage keyValueMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
				
				//1 -> message from client
				if(keyValueMsg.getConnection() == 1) {
					
					System.out.println("Received message from Client...");
					Thread coordinatorThread = new Thread(new Coordinator(request, sc, keyValueMsg));
					coordinatorThread.start();
					
				}
				
				//0 -> message from replica servers
				if(keyValueMsg.getConnection() == 0){
					
				//	System.out.println("Received message from Server...");
					
					String receiveServer = keyValueMsg.getServerName();
					
				//	sc.addConnectedServers(receiveServer, true);
					
					KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
					OutputStream out = null;
					
					if(keyValueMsg.hasPutKey()) {
						
						hintedhandoff(receiveServer, sc);
						
						KeyValue.Put put = keyValueMsg.getPutKey();
						
					//	Random rand = new Random();
					//	int randomNum = rand.nextInt((5 - 1)) + 1;
						KeyValue.KeyValuePair keyStore = put.getKeyval();

						String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
						wrlog.writeToFile(writeAheadLog);						
						sc.store.put(keyStore.getKey(), keyStore);							
						System.out.println("Message written to keyStore..!!\n");
						sc.printStore();
							
						KeyValue.WriteResponse.Builder wr = KeyValue.WriteResponse.newBuilder();
						wr.setKey(keyStore.getKey());
						wr.setId(put.getId());
						wr.setWriteReply(true);
						
						//reply back to co-ordinator after message written to keystore
						out = request.getOutputStream();
						keyMessage.setWriteResponse(wr.build());
						keyMessage.build().writeDelimitedTo(out);
						out.flush();
						out.close();
						
					}
										
					if(keyValueMsg.hasReadRepair()) {
						
						KeyValue.ReadRepair rr = keyValueMsg.getReadRepair();
						KeyValue.KeyValuePair keyStore = rr.getKeyval();
						int key = keyStore.getKey();
						
						String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
						wrlog.writeToFile(writeAheadLog);
						sc.store.put(key, keyStore);						
						System.out.println("Read Repair done...");
						sc.printStore();
					
					}
										
					if(keyValueMsg.hasHintedHandoff()) {
						
						KeyValue.HintedHandoff hh = keyValueMsg.getHintedHandoff();
						KeyValue.KeyValuePair keyStore = hh.getKeyval();
						int key = keyStore.getKey();
						
						if(sc.store.containsKey(key) && keyStore.getTime() >= sc.store.get(key).getTime()) {
							String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
							wrlog.writeToFile(writeAheadLog);
							sc.store.replace(key, keyStore);
							System.out.println("Hinted Handoff done...");
							sc.printStore();
						}
						else {
							String writeAheadLog = keyStore.getKey() + " " + keyStore.getValue() + " " + keyStore.getTime();
							wrlog.writeToFile(writeAheadLog);
							sc.store.put(key, keyStore);
							System.out.println("Hinted Handoff done...");
							sc.printStore();
						}					
					}
					
					if(keyValueMsg.hasGetKey()) {
						
						hintedhandoff(receiveServer, sc);
						
						int key = keyValueMsg.getGetKey().getKey();
						KeyValue.KeyValuePair keyStore = null;
						KeyValue.ReadResponse.Builder readResp = KeyValue.ReadResponse.newBuilder();
						
						if(sc.store.containsKey(key)) {
							keyStore = sc.store.get(key);
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
						//reply back to co-ordinator...
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	private static void hintedhandoff(String serverName,ServerContext sc) {
		
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
