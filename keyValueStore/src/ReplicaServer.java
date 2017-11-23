import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStream;

public class ReplicaServer{
	
	public static void main(String[] args){

		if(args.length != 2){
			System.out.println("Usage: ./server server1 9090 conf.txt\n");
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
		Thread cl = new Thread();
		
		Socket request = null;
		try {
			request = sc.server.accept();
			InputStream in = request.getInputStream();
			KeyValue.KeyValueMessage incoming = KeyValue.KeyValueMessage.parseDelimitedFrom(in);
			if(incoming.getConnection() == 1) {
				sc.handleClient(incoming);
			}
			cl = new Thread(new Coordinator(request, sc));
			cl.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
}
