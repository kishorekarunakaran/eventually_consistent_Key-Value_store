package keyValueStore.server;

import java.util.HashMap;

/**
* 
* @author  Surendra kumar Koneti
* @since   2017-11-21
*/

public class ReadRepair {
	
	private int id = 0;
	private int key = 0;
	private String value = null;
	private long timestamp = 0;
	private HashMap<String,Boolean> servers = new HashMap<String,Boolean>();
	public HashMap<String,Long> serversTimestamp = new HashMap<String,Long>();
	private Boolean readRepairStatus = false;
	private Boolean readStatus = false;
	
	/**
	 * 
	 * @param idIn unique id for a key Value pair message
	 * @param keyIn Key to be inserted
	 * @param valueIn Value of the key to be inserted
	 * @param timeIn TimeStamp of the key value pair message recorded
	 */	
	public ReadRepair(int idIn, int keyIn, String valueIn, long timeIn) {
		id = idIn;
		key = keyIn;
		value = valueIn;
		timestamp = timeIn;		
	}

	/**
	 * 
	 * @return key returns the key for which read repair has to be done
	 */
	public int getKey() {
		return key;
	}

	/**
	 * 
	 * @param key Key for which read repair has to be done.
	 */
	public void setKey(int key) {
		this.key = key;
	}

	/**
	 * 
	 * @return value returns the value recorded
	 */
	public String getValue() {
		return value;
	}

	/**
	 * 
	 * @param value Value of the key
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * 
	 * @return timestamp of the key value message recorded
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * 
	 * @param timestamp Time recorded for the key value message
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public HashMap<String,Boolean> getServers() {
		return servers;
	}

	public void addServers(String serverName, Boolean b) {
		servers.put(serverName, b);
	}

	public void updateServers() {
		for(String name: servers.keySet()) {
			servers.replace(name, false);
		}
	}

	/**
	 * 
	 * @return readRepairStatus whether the read repair has to be done for a key value message for any server
	 */
	public Boolean getReadRepairStatus() {
		return readRepairStatus;
	}

	/**
	 * 
	 * @param statusIn Status for the key Value message pair, true->read repair has to be done, false->not required
	 */
	public void setReadRepairStatus(Boolean statusIn) {
		readRepairStatus = statusIn;
	}

	public Boolean getReadStatus() {
		return readStatus;
	}

	public void setReadStatus(Boolean readStatus) {
		this.readStatus = readStatus;
	}
	
	/**
	 * 
	 * @param consistencyIn Consistency value set by user, Checks if the consistency requirements are met
	 * @return Boolean whether consistency level is met or not
	 */
	public Boolean checkConsistency(int consistencyIn) {
		int value = 0;
		for(String name: serversTimestamp.keySet()) {
			if(serversTimestamp.get(name) == timestamp) {
				value++;
				if(value == consistencyIn) {
					return true;
				}
			}
		}
		return false;
	}

}
