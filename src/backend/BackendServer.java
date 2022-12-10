package backend;

import static webserver.Server.*;

import java.net.URLDecoder;
import java.util.List;
import kvs.KVSClient;
import tools.Hasher;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
import json.org.json.simple.JSONArray;
import json.org.json.simple.JSONObject;

public class BackendServer {

	public static void main(String[] args) {
		System.out.println("Starting Backend ... ");
		if (args.length != 2) {
			System.err.println("Syntax: Backend <port> <kvsMaster>");
			System.exit(1);
		}
		int myPort = Integer.valueOf(args[0]);
		KVSClient kvs = new KVSClient(args[1]);

		port(myPort);
		System.out.println("Backend listening on " + myPort + " ... ");

		get("/search", (req, res) -> {
			// this header is needed to for CORS
			res.header("Access-Control-Allow-Origin", "*");
			String query = URLDecoder.decode(req.queryParams("query").toLowerCase(), "UTF-8");
			System.out.println("Query is: " + query);
			String[] searchTerms = query.split("\\s+");
			JSONObject results = new JSONObject();
			results.put("results", null);
			List<String> outputURLs = Ranker.rank(kvs, searchTerms);
			JSONArray list = new JSONArray();
			for (String url : outputURLs) {
				try {
					// Create JSON Object to add attribute URL and content
					JSONObject data = new JSONObject();
					String hashURL = Hasher.hash(url);
					String content = new String(kvs.get("content", hashURL, "page"));
					content = content.replaceAll("[.,:;!\\?\'\"()-]", " ").replaceAll("<[^>]*>", "");

					StringBuilder previewContent = new StringBuilder();
					String[] splitContent = content.split("\\s+");

					// Add only a small preview of the content
					for (int i = 0; i < Math.min(100, splitContent.length); i++) {
						previewContent.append(splitContent[i] + " ");
					}

					data.put("URL", url);
					data.put("content", previewContent.toString());
					list.add(data);
				} catch (Exception e) {
					System.out.println("Exception: " + e);
				}
			}
			results.put("results", list);
			return results;
//			return "OK";
		});
	}
}
