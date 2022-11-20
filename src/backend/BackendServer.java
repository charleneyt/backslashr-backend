package backend;

import static webserver.Server.*;

import flame.FlameContext;
import kvs.KVSClient;
import kvs.Row;
import org.json.simple.*;

public class BackendServer {

	public static void main(String[] args) {
	    if (args.length != 2) {
	        System.err.println("Syntax: Backend <port> <kvsMaster>");
	        System.exit(1);
	    }
	    int myPort = Integer.valueOf(args[0]);
	    KVSClient kvs = new KVSClient(args[1]);
	    
	    port(myPort);
	    System.out.println("backend listening on " + myPort + " !");
	   
	    get("/search", (req,res) -> { 
	    	// this header is needed to for CORS
	    	res.header("Access-Control-Allow-Origin", "*");
	    	String query = req.queryParams("query");
	    	System.out.println("query is: " + query);
	    	JSONObject results = new JSONObject();
	    	results.put("results", null);
		    
		    try {
		    	Row row = kvs.getRow("index", query);
		    	if (row == null) {	
		    		return results;
		    	}
		    	
		    	String[] urls = row.get("value").split(",");
		    	JSONArray list = new JSONArray();
		    	for (String result : urls) {
		    		int idx = result.lastIndexOf(":");
		    		if (idx < 0)
		    			list.add(result);
		    		else {
		    			list.add(result.substring(0, idx));
		    		}
		    	}
		    	
		    	results.put("results", list);
		    	
		    	return results;
		    } catch (Exception e) {
		    	System.out.println("Failed to get row.");
		    	if (kvs == null) {
		    		System.out.println("kvs is null!!!");
		    	}
	    		return results;
		    }
	    	
//	    	
//	    	// simple logic just to confirm backend can get the query string
//	    	if (query.equals("upenn")) {
//	    		return "[\"https://www.upenn.edu/\"]"; 
//	    	}
//
//	    	return "[\"http://simple.crawltest.cis5550.net\", \"https://www.upenn.edu/\"]"; 
	    	
	    });
	}

}
