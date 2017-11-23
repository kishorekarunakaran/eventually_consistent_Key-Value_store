import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class Coordinator implements Runnable{
	
	Socket socket = null;
	ServerContext sc = null;
	
	public Coordinator(Socket in, ServerContext scIn) {
		socket = in;
		sc = scIn;
	}

	@Override
	public void run() {
		
		while(true) {
			try {
				InputStream in = socket.getInputStream();
				KeyValue.KeyValueMessage incoming = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
				if(incoming != null) {
					sc.handleClient(incoming);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
			
			
		}
		
	}

}
