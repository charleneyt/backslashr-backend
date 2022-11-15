package webserver;

public class HostRecord {
	public String host;
	public String jksFile;
	public String password;
	
	public HostRecord(String host, String jksFile, String password) {
		this.host = host;
		this.jksFile = jksFile;
		this.password = password;
	}
}
