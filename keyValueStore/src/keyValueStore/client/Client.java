package keyValueStore.client;

import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Scanner;

import keyValueStore.keyValue.KeyValue;
import keyValueStore.util.FileProcessor;
import keyValueStore.util.uniqueIdGenerator;

public class Client{
		
	public static void main(String [] args) {
					
		ClientContext cc = new ClientContext(args[0]);
		FileProcessor fp = new FileProcessor(args[1]);
	    
		cc.readFile(fp);
		
		Scanner scan = new Scanner(System.in);
		
		Thread receive = new Thread(new Runnable() {

			@Override
			public void run() {
				while(true) {
					try {
						InputStream in = cc.sock.getInputStream();
						KeyValue.KeyValueMessage incomingMsg = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
						
						if(incomingMsg != null) {
							
							System.out.println("\nReceived response from co-ordinator..!!");

							if(incomingMsg.hasWriteResponse()) {
								
								KeyValue.WriteResponse wr = incomingMsg.getWriteResponse();
								System.out.println(wr.getKey() + " " + wr.getWriteReply());
								
							}
						
							if(incomingMsg.hasReadResponse()) {
								
								KeyValue.ReadResponse readResponse = incomingMsg.getReadResponse();
								KeyValue.KeyValuePair keyStore = readResponse.getKeyval();
								
								if(readResponse.getReadStatus()) {
									System.out.println(keyStore.getKey() + " " + keyStore.getValue());
								}
								else {
									System.out.println("Key " + keyStore.getKey() + " not present in KeyValue store" );
								}
							
							}
							
							if(incomingMsg.hasException()) {
								
								KeyValue.Exception ex = incomingMsg.getException();
								System.out.println("Exception : " + ex.getMethod() + " Method");
								System.out.println("\t" + ex.getExceptionMessage());
								
							}
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			}			
		});
		
		while(true) {
			String value = scan.next();
			if(value == null){
				System.out.println("Enter correct input");
				break;	
			}
		
			String[] splitValue;
			splitValue = value.split(",");
			
			KeyValue.KeyValueMessage.Builder keyMessage = KeyValue.KeyValueMessage.newBuilder();
			try {
			
			if(splitValue[0].equalsIgnoreCase("get")) {
				
				int key = Integer.parseInt(splitValue[1]);
				int clevel = Integer.parseInt(splitValue[2]);
				
				KeyValue.Get.Builder getMethod = KeyValue.Get.newBuilder();
				getMethod.setKey(key);
				getMethod.setConsistency(clevel);
				getMethod.setId(uniqueIdGenerator.getUniqueId());
				
				keyMessage.setGetKey(getMethod.build());
				
			}
			if(splitValue[0].equalsIgnoreCase("put")) {
				
				int key = Integer.parseInt(splitValue[1]);
				String input = splitValue[2];
				int clevel = Integer.parseInt(splitValue[3]);
				
				KeyValue.KeyValuePair.Builder keyStore = KeyValue.KeyValuePair.newBuilder();
				keyStore.setKey(key);
				keyStore.setValue(input);
				
				KeyValue.Put.Builder putMethod = KeyValue.Put.newBuilder();
				putMethod.setKeyval(keyStore.build());
				putMethod.setConsistency(clevel);
				putMethod.setId(uniqueIdGenerator.getUniqueId());
				
				keyMessage.setPutKey(putMethod.build());
			}
			} catch(NumberFormatException i) {
				System.out.println("Wrong input...");
				continue;
			} catch(Exception e) {
				System.out.println("Wrong input...");
				continue;
			}
			try {				
				if(cc.sock == null) {
					cc.sock = new Socket(cc.coIp,cc.coPort);
					keyMessage.setConnection(1);
					receive.start();
				}
				
				OutputStream out = cc.sock.getOutputStream();
				keyMessage.build().writeDelimitedTo(out);
				out.flush();
				
			} catch(ConnectException e) {
				System.out.println("Server down....");
			//	e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Server down....");
			//	e.printStackTrace();
			}
		}	
	}
}
