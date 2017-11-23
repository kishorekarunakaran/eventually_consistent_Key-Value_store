import java.net.Socket;
import java.net.UnknownHostException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Scanner;

public class Client{
		
	public static void main(String [] args) {
					
		ClientContext cc = new ClientContext(args[0]);
		FileProcessor fp = new FileProcessor(args[1]);
	    
		cc.readFile(fp);
		
		Scanner scan = new Scanner(System.in);
		
		while(true) {
			String value = scan.next();
			if(value == null){
				System.out.println("Enter correct input");
				break;	
			}
			System.out.println(value);
			String[] splitValue;
			splitValue = value.split(",");
			KeyValue.KeyValueMessage.Builder keymessage = KeyValue.KeyValueMessage.newBuilder();
			if(splitValue[0].equalsIgnoreCase("get")) {
				int key = Integer.parseInt(splitValue[1]);
				int clevel = Integer.parseInt(splitValue[2]);
				KeyValue.Get.Builder getMethod = KeyValue.Get.newBuilder();
				getMethod.setKey(key);
				getMethod.setConsistency(clevel);
				keymessage.setGetKey(getMethod.build());
			}
			if(splitValue[0].equalsIgnoreCase("put")) {
				System.out.println("here");
				int key = Integer.parseInt(splitValue[1]);
				String input = splitValue[2];
				int clevel = Integer.parseInt(splitValue[3]);
				KeyValue.Put.Builder putMethod = KeyValue.Put.newBuilder();
				putMethod.setKey(key);
				putMethod.setValue(input);
				putMethod.setConsistency(clevel);
				keymessage.setPutKey(putMethod.build());
			}
			keymessage.setConnection(1);
			try {
				Socket send;
				if(cc.sock == null) {
					System.out.println("Socket");
					send = new Socket(cc.coIp,cc.coPort);
					cc.setSocket(send);
				}
				else {
					send = cc.getSocket();
				}
				OutputStream out = send.getOutputStream();
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
