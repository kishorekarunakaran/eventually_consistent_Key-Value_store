package keyValueStore.util;

import java.util.Random;

/**
* 
* @author  Surendra kumar Koneti
* @since   2017-11-21
* Description: This program generates an unique id.
*/

public class uniqueIdGenerator {
	
	/**
	 * @return id returns an unique identifier 
	 */
	public synchronized static int getUniqueId() {
	
		Random rand = new Random();
		int id = rand.nextInt(Integer.MAX_VALUE);
		return id;
	}
}
