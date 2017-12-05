package keyValueStore.server;

import java.util.HashMap;

public class ReadRepair {
	
	private int id = 0;
	private int key = 0;
	private String value = null;
	private long timestamp = 0;
	private HashMap<String,Boolean> servers = new HashMap<String,Boolean>();
	public HashMap<String,Long> serversTimestamp = new HashMap<String,Long>();
	private Boolean readRepairStatus = false;
	private Boolean readStatus = false;
	
	public ReadRepair(int idIn, int keyIn, String valueIn, long timeIn) {
		id = idIn;
		key = keyIn;
		value = valueIn;
		timestamp = timeIn;		
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

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

	public Boolean getReadRepairStatus() {
		return readRepairStatus;
	}

	public void setReadRepairStatus(Boolean statusIn) {
		readRepairStatus = statusIn;
	}

	public Boolean getReadStatus() {
		return readStatus;
	}

	public void setReadStatus(Boolean readStatus) {
		this.readStatus = readStatus;
	}
	
	public Boolean checkConsistency(int con) {
		int value = 0;
		for(String name: serversTimestamp.keySet()) {
			if(serversTimestamp.get(name) == timestamp) {
				value++;
			}
		}
		if(value == con) {
			return true;
		}
		return false;
	}

}
