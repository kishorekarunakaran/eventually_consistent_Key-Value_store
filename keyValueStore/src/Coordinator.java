import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;

public class Coordinator implements Runnable{
	
	Socket socket = null;
	ServerContext sc = null;
	HashMap<Integer,Integer> con = new HashMap<Integer,Integer>();
	
	public Coordinator(Socket in, ServerContext scIn, KeyValue.KeyValueMessage km) {
		socket = in;
		sc = scIn;
		handleClient(km);
	}
	
	public void handleClient(KeyValue.KeyValueMessage incoming) {
		if(incoming.hasPutKey()) {
			KeyValue.Put put = null;
			put = incoming.getPutKey();
			//System.out.println(put.getKey() + " " + put.getValue() + " " + put.getConsistency());
			KeyValue.Put.Builder putserver = KeyValue.Put.newBuilder();
			putserver.setKey(put.getKey());
			putserver.setValue(put.getValue());
			putserver.setConsistency(put.getConsistency());
			Date date = new Date();
			long time = date.getTime();
			putserver.setTime(time);
			con.put(putserver.getKey(),putserver.getConsistency());
			KeyValue.KeyValueMessage.Builder km = KeyValue.KeyValueMessage.newBuilder();
			km.setConnection(0);
			km.setPutKey(putserver).build();		
			sendToServers(km);
		}
	}

	public void sendToServers(KeyValue.KeyValueMessage.Builder in) {
		
		for(String temp: sc.serversIp.keySet()) {
			try {
				Socket send = new Socket(sc.serversIp.get(temp), sc.serversPort.get(temp));
				OutputStream out = send.getOutputStream();
				in.build().writeDelimitedTo(out);
				out.flush();
				new Thread(new Runnable(){
					
					public void run() {
						try {
							InputStream re = send.getInputStream();
							KeyValue.KeyValueMessage response = KeyValue.KeyValueMessage.parseDelimitedFrom(re);
							if(response != null) {
								KeyValue.WriteResponse wr = response.getWriteResponse();
								int key = wr.getId();
								if(wr.getWriteReply() && ( con.get(key) > 0 )) {
									int val = con.get(key);
									val = val - 1;
									con.replace(key,val);
									System.out.println("Response received" + " " + con.get(key) + " " + wr.getId());
								}
								if(con.get(key) == 0) {
									con.replace(key, -1);
									KeyValue.KeyValueMessage.Builder res = KeyValue.KeyValueMessage.newBuilder();
									res.setWriteResponse(wr);
									OutputStream out = socket.getOutputStream();
									res.build().writeDelimitedTo(out);
								}
							}
						}catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}).start();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}	

	@Override
	public void run() {
		
		while(true) {
			try {
				InputStream in = socket.getInputStream();
				KeyValue.KeyValueMessage incoming = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
				if(incoming != null) {
					handleClient(incoming);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}						
		}
	}
}
