package keyValueStore.util;

import java.util.Random;

public class uniqueIdGenerator {
	
	public synchronized static int getUniqueId() {
	
		Random rand = new Random();
		int id = rand.nextInt(Integer.MAX_VALUE);
		return id;
	}
}
