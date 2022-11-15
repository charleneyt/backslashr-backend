package generic;

import static webserver.Server.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;


public class Master {
	final static String LF = "\n";

	public static Map<String, WorkerEntry> workers = new ConcurrentSkipListMap<>((a, b) -> a.compareTo(b));
	public static List<String> workerNames = new ArrayList<>();
	public static String currWorkerString = "0\n";
	public static int workersCount = 0;
	public static long lastUpdated = 0;
	
	/**
	 * 
	 * @return the current list of workers
	 */
	public static Vector<String> getWorkers(){
		Vector<String> vector = new Vector<>();
		for (WorkerEntry we : workers.values()) {
			vector.add(we.ip + ":" + we.port);
		}
		return vector;
	}
	
	/**
	 * 
	 * @return the HTML table lists each worker's ID, IP, port, and hyperlink
	 */
	public static String workerTable() {
		StringBuilder sb = new StringBuilder();
		sb.append("<table><tr><td>ID</td><td>IP</td><td>Port</td><td>Hyperlink</td></tr>");
		Iterator<WorkerEntry> iter = workers.values().iterator();
		while (iter.hasNext()){
			WorkerEntry workerEntry = iter.next();
			if (workerEntry.lastPinged == -1 || workerEntry.lastPinged + 15000 < System.currentTimeMillis()){
				workers.remove(workerEntry.id);
			} else {
				InetSocketAddress link = new InetSocketAddress(workerEntry.ip, workerEntry.port);
				sb.append("<tr><td>" + workerEntry.id + "</td><td>" + workerEntry.ip + "</td><td>" + workerEntry.port + "</td><td><a href=\"http:/" + link.toString() + "\">http:/" + link.toString() + "</td></tr>");
			}
		}

		synchronized (Master.class){
			workersCount = workers.size();
			workerNames = new ArrayList<>(workers.keySet());
			lastUpdated = System.currentTimeMillis();
		}

		sb.append("</table>");
		return sb.toString();
	}
	
	/**
	 * creates routes like /ping and /workers routes (but not the / route)
	 */
	public static void registerRoutes() {
		// http://xxx/ping?id=yyy&port=zzz input will be coming from Worker, with queryparams id and port
		get("/ping", (req, res) -> {
			// The request should return a 400 error if the ID and/or the port number are missing, and the string OK otherwise.
			if (!req.queryParams().contains("id") || !req.queryParams().contains("port")){
				res.status(400, "Bad Request");
				return "400 Bad Request";
			}
			String id = req.queryParams("id"); 
			if (id.contains(", ")){
				String[] ids = id.split(", ");
				id = ids[ids.length - 1];
			}
			String portStr = req.queryParams("port");
			if (portStr.contains(", ")){
				String[] ports = portStr.split(", ");
				portStr = ports[ports.length - 1];
			}
			int port = Integer.valueOf(portStr); 
			// if an entry for x already exists, its IP and port should be updated

			if (workers.containsKey(id)){
				workers.get(id).updateIpAndPort(req.ip(), port);
			} else {
				WorkerEntry worker = new WorkerEntry(id, req.ip(), port);
				workers.put(id, worker);
			}

			workers.get(id).updatePingedTime(System.currentTimeMillis());
			
			return "OK";
		});

		get("/workers", (req, res) -> {
			// return k + 1 lines of text, separated by LFs; the first line should contain k, and each of the following lines should contain an entry for a different worker, in the format id,ip:port
			if (lastUpdated == 0 || lastUpdated + 5000 < System.currentTimeMillis()){
				lastUpdated = System.currentTimeMillis();
				StringBuilder sb = new StringBuilder();

				Iterator<WorkerEntry> iter = workers.values().iterator();
				while (iter.hasNext()){
					WorkerEntry workerEntry = iter.next();
					// When a worker has not made a /ping request within the last 15 seconds, it should be considered inactive, and be removed from the list
					if (workerEntry.lastPinged == -1 || workerEntry.lastPinged + 15000 < System.currentTimeMillis()){
						workers.remove(workerEntry.id);
					} else {
						sb.append(workerEntry.id + "," + workerEntry.ip + ":" + workerEntry.port + Master.LF);
					}
				}

				synchronized (Master.class){
					workersCount = workers.size();
					workerNames = new ArrayList<>(workers.keySet());
				}
				
				// remove the last LF if activeCount is at least 1
				if (workersCount > 0){
					sb.delete(sb.length() - 1, sb.length());
				}
				currWorkerString = workersCount + Master.LF + sb.toString();
			}
			return currWorkerString;
		});
	}
}
