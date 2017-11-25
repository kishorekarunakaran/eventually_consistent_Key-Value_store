package keyValueStore.server;

public class KeyStore{
	
	private int key = 0;
	private String value = null;
	private long timestamp = 0;
	
	public KeyStore(int keyIn, String valueIn, long timeIn) {
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

}
