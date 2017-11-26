package keyValueStore.server;

import java.util.HashMap;

public class ReadRepair {
	
	private int id = 0;
	private int key = 0;
	private String value = null;
	private long timestamp = 0;
	private HashMap<String,Boolean> servers = new HashMap<String,Boolean>();
	private Boolean readRepairStatus = false;
	
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

	public void setReadRepairStatus(Boolean readRepairStatusIn) {
		readRepairStatus = readRepairStatusIn;
	}
}
