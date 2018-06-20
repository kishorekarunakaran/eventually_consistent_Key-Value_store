package keyValueStore.client;

import java.net.Socket;
import keyValueStore.util.FileProcessor;

/**
* 
* @author  Surendra kumar Koneti
* @since   2017-11-22
*/

public class ClientContext {
	
	private String coordinator = null;
	static String coIp = null;
	static int coPort = 0;
	static Socket sock = null;
	
	/**
	 * 
	 * @param coor user defined value, the coordinator to which the client needs to communicate
	 */

	public ClientContext(String coor) {
		coordinator = coor;
	}
	
	/**
	 * 
	 * @param fp util.fileProcessor instance
	 */
	
	public void readFile(FileProcessor fp) {
		
		while(true){
			String value = fp.readLine();
			if(value == null){
				break;	
			}
			String[] splitValue;
			splitValue = value.split(" ");
			if(coordinator.equalsIgnoreCase(splitValue[0])) {
				coIp = splitValue[1];
				coPort = Integer.parseInt(splitValue[2]);
			}		
		}

		System.out.println("Co-ordinator Info -> "  + coIp + " " + coPort);
	}
		
}
