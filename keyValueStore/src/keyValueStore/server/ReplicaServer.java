package keyValueStore.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;

import keyValueStore.keyValue.KeyValue;
import keyValueStore.util.FileProcessor;

public class ReplicaServer{
	
	public static void main(String[] args){

		if(args.length != 3){
			System.out.println("Usage: ./server.sh <server name> <port> <config file>\n");
			System.exit(0);
		}
		
		ServerContext sc = new ServerContext(args[0],Integer.parseInt(args[1]));
		FileProcessor fp = new FileProcessor(args[2]);
		sc.readFile(fp);
		
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
					}
					
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
