package generic;

public class WorkerEntry{
	String id;
	String ip;
	int port;
	long lastPinged = -1;

	public WorkerEntry(String id, String ip, int port){
		this.id = id;
		this.ip = ip;
		this.port = port;
	}

	public void updateIpAndPort(String ip, int port){
		this.ip = ip;
		this.port = port;
	}

	public void updatePingedTime(long time){
		lastPinged = time;
	}

	@Override
	public String toString(){
		return ip + ":" + port;
	}
}
