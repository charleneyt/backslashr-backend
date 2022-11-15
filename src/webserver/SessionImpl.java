package webserver;

import java.util.*;

public class SessionImpl implements Session {
	final static int DEFAULT_INTERVAL = 300;

	public String id;
	public long creationTime;
	public long lastAccessedTime;
	public int maxActiveInterval = DEFAULT_INTERVAL;
	Map<String, Object> pairs;
	public boolean valid = true;

	public SessionImpl(String sessionId){
		id = sessionId;
		creationTime = System.currentTimeMillis();
		lastAccessedTime = creationTime;
		pairs = new HashMap<>();
	}


	@Override
	public String id() {
		return id;
	}

	@Override
	public long creationTime() {
		return creationTime;
	}

	@Override
	public long lastAccessedTime() {
		return lastAccessedTime;
	}

	public void setLastAccessedTime(long time) {
		lastAccessedTime = time;
	}

	@Override
	public void maxActiveInterval(int seconds) {
		// convert to milliseconds
		this.maxActiveInterval = seconds;
	}

	@Override
	public void invalidate() {
		valid = (lastAccessedTime + 1000 * maxActiveInterval >= System.currentTimeMillis());
	}

	@Override
	public Object attribute(String name) {
		if (valid){
			return pairs.getOrDefault(name, null);
		}
		return null;
	}

	@Override
	public void attribute(String name, Object value) {
		pairs.put(name, value);
	}

}
