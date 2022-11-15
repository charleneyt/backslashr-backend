package kvs;

import static webserver.Server.*;


public class Master extends generic.Master {
	public static void main(String[] args) {
		// check input
		if (args.length != 1) {
			return;
		}

		Master master = new Master();
		// use port and register routes
		port(Integer.valueOf(args[0]));
		master.registerRoutes();
		// return a HTML page with a table that contains an entry for each active worker and lists its ID, IP, and port. Each entry should have a hyperlink to http://ip:port/, where ip and port are the IP and port number of the corresponding worker.
		get("/", (req, res) -> {
			res.type("text/html");
			return "<!doctype html><html><head><title>KVS Master</title></head><body><div>KVS Workers List</div>" 
											+ master.workerTable()
											+ "</body></html>";
		});
	}
}
