package backend;

import static webserver.Server.*;

import java.net.URLDecoder;
import java.util.List;
import kvs.KVSClient;
import tools.Hasher;

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
			System.out.println("Total results = " + outputURLs.size());
			JSONArray list = new JSONArray();
			for (String url : outputURLs) {
				try {
					// Create JSON Object to add attribute URL and content
					JSONObject data = new JSONObject();

//					Document doc = Jsoup.connect(url).get();
//					String title = doc.title();
//					StringBuilder previewContent = new StringBuilder();
//					int len = 0;
//
//					String text = doc.body().text();
//					previewContent.append(text);

//					Elements paragraphs = doc.select("p");
//					for (Element p : paragraphs) {
//						if (len > 100) {
//							break;
//						}
//						previewContent.append(p.text());
//						len++;
//					}
//					data.put("title", title);
					String hashURL = Hasher.hash(url);
					StringBuilder previewContent = new StringBuilder();
					if (kvs.get("content", hashURL, "page") != null) {
						String content = new String(kvs.get("content", hashURL, "page"));
						content = content.replaceAll("[.,:;!\\?\'\"()-]", " ").replaceAll("<[^>]*>", "");
						String[] splitContent = content.split("\\s+");

						// Add only a small preview of the content
						for (int i = 0; i < Math.min(100, splitContent.length); i++) {
							previewContent.append(splitContent[i] + " ");
						}
					}
					String preview = previewContent.length() > 0 ? previewContent.toString()
							: "No preview content available ";
					data.put("URL", url);
					data.put("content", preview);
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
