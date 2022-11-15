package generic;

import java.util.Random;

import tools.HTTP;

public class Worker implements Runnable {
	final static String LOWER_STRING = "abcdefghijklmnopqrstuvwxyz";
	
	public String id;
	public int port;
	public String masterAddrAndPort;

	public Worker(String id, int port) {
		this.id = id;
		this.port = port;
	}


	public void updateMasterIpAndPort(String masterAddrAndPort){
		this.masterAddrAndPort = masterAddrAndPort;
	}
	
	protected static String generateId(int length){
		Random rand = new Random();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < length; i++){
			sb.append(LOWER_STRING.charAt(rand.nextInt(26)));
		}
		return sb.toString();
	}
	
	/**
	 * creates a thread that makes the periodic /ping requests
	 */
	public void startPingThread() {
		new Thread(this).start();
	}

	@Override
	public void run() {
		while (true){
			try {
				// URL urlReq = new URL("http://" + masterAddrAndPort + "/ping?id=" + id + "&port=" + port);
				// // urlReq.getContent();
				// HttpURLConnection conn = (HttpURLConnection) urlReq.openConnection();
				// conn.setRequestMethod("GET");
				// conn.setRequestProperty("Connection", "close");
				// BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				// String line;
				// while ((line = in.readLine()) != null){}
				// in.close();
				// conn.disconnect();

				HTTP.doRequest("GET", "http://" + masterAddrAndPort + "/ping?id=" + java.net.URLEncoder.encode(id, "UTF-8") + "&port=" + port, null);
				Thread.sleep(5000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
