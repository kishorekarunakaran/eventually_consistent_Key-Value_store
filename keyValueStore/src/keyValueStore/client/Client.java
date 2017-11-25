package keyValueStore.client;

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
						System.out.println("");
						System.out.println("Received response from co-ordinator..!!");

						if(incomingMsg.hasWriteResponse()) {
							KeyValue.WriteResponse wr = incomingMsg.getWriteResponse();
							System.out.println(wr.getKey() + " " + wr.getWriteReply());
						}
						
						if(incomingMsg.hasReadResponse()) {
							KeyValue.ReadResponse readResponse = incomingMsg.getReadResponse();
							
							if(!readResponse.getValue().equals("EMPTY"))
								System.out.println(readResponse.getKey() + " " + readResponse.getValue());
							else
								System.out.println("Key " + readResponse.getKey() + " not present in KeyValue store" );
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
		//	System.out.println(value);
			String[] splitValue;
			splitValue = value.split(",");
			KeyValue.KeyValueMessage.Builder keymessage = KeyValue.KeyValueMessage.newBuilder();
			if(splitValue[0].equalsIgnoreCase("get")) {
				int key = Integer.parseInt(splitValue[1]);
				int clevel = Integer.parseInt(splitValue[2]);
				KeyValue.Get.Builder getMethod = KeyValue.Get.newBuilder();
				getMethod.setKey(key);
				getMethod.setConsistency(clevel);
				getMethod.setId(uniqueIdGenerator.getUniqueId());
				keymessage.setGetKey(getMethod.build());
			}
			if(splitValue[0].equalsIgnoreCase("put")) {
				int key = Integer.parseInt(splitValue[1]);
				String input = splitValue[2];
				int clevel = Integer.parseInt(splitValue[3]);
				KeyValue.Put.Builder putMethod = KeyValue.Put.newBuilder();
				putMethod.setKey(key);
				putMethod.setValue(input);
				putMethod.setConsistency(clevel);
				putMethod.setId(uniqueIdGenerator.getUniqueId());
				keymessage.setPutKey(putMethod.build());
			}
			try {
				//Socket send;
				if(cc.sock == null) {
					cc.sock = new Socket(cc.coIp,cc.coPort);
					keymessage.setConnection(1);
					//cc.setSocket(send);
					receive.start();
				}
				else {
					//send = cc.getSocket();
				}
				OutputStream out = cc.sock.getOutputStream();
				keymessage.build().writeDelimitedTo(out);
				out.flush();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}
		
	}

}
