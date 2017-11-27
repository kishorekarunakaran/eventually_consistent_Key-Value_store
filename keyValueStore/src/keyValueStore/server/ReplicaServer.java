package keyValueStore.server;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
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
		
		Socket request = null;
		while(true) {
			try {
				System.out.println("Listening on " + InetAddress.getLocalHost().getHostAddress() +" " + + sc.port);
				
				request = sc.server.accept();
				System.out.println("\nNew connection accepted");
				
				InputStream in = request.getInputStream();
				KeyValue.KeyValueMessage keyValueMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
				
				//1 -> message from client
				if(keyValueMsg.getConnection() == 1) {
					System.out.println("Received msg from Client");
					Thread coordinatorThread = new Thread(new Coordinator(request, sc, keyValueMsg));
					coordinatorThread.start();
				}
				
				//0 -> message from replica servers
				if(keyValueMsg.getConnection() == 0){
					System.out.println("Received msg from Server");
					KeyValue.KeyValueMessage.Builder keyValMsgBuilder = KeyValue.KeyValueMessage.newBuilder();
					KeyValue.WriteResponse.Builder wr = KeyValue.WriteResponse.newBuilder();
					OutputStream out = null;
					
					if(keyValueMsg.hasPutKey()) {
						KeyValue.Put put = keyValueMsg.getPutKey();
						
					//	Random rand = new Random();
					//	int randomNum = rand.nextInt((5 - 1)) + 1;
						
						String writeAheadLog = put.getKey() + " " + put.getValue() + " " + put.getTime();
						wrlog.writeToFile(writeAheadLog);
					
						if(put.getReadRepair() == 1) {
							
							KeyStore temp = new KeyStore(put.getKey(), put.getValue(), put.getTime());	
							sc.store.put(put.getKey(),temp);
							
							System.out.println("Message updated to keyStore..!!");
							sc.printStore();
							in.close();
							request.close();
							
						}
						else {
							
							KeyStore temp = new KeyStore(put.getKey(), put.getValue(), put.getTime());	
							sc.store.put(put.getKey(),temp);
							
							System.out.println("Message written to keyStore..!!");
							sc.printStore();
							
							wr.setKey(put.getKey());
							wr.setId(put.getId());
							wr.setWriteReply(true);
						
							//reply back to co-ordinator after message written to keystore
							out = request.getOutputStream();
							keyValMsgBuilder.setWriteResponse(wr.build());
							keyValMsgBuilder.build().writeDelimitedTo(out);
							out.flush();
							out.close();
							in.close();
							request.close();
							
						}
					}
					
					if(keyValueMsg.hasGetKey()) {
						int key = keyValueMsg.getGetKey().getKey();
						KeyStore keyValueObj = sc.store.get(key);
						
						KeyValue.ReadResponse.Builder readResp = KeyValue.ReadResponse.newBuilder();
						if(keyValueObj != null) {
							readResp.setKey(keyValueObj.getKey());
							readResp.setValue(keyValueObj.getValue());
							readResp.setTime(keyValueObj.getTimestamp());
							readResp.setId(keyValueMsg.getGetKey().getId());
						}else {
							readResp.setKey(key);
							readResp.setValue("EMPTY");
							readResp.setId(keyValueMsg.getGetKey().getId());
						}
						
						//reply back to co-ordinator after message written to keystore
						out = request.getOutputStream();
						keyValMsgBuilder.setReadResponse(readResp);
						keyValMsgBuilder.build().writeDelimitedTo(out);
						out.flush();
						out.close();
						in.close();
						request.close();
					}
					
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
