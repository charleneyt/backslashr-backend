package backend;

import static webserver.Server.*;

import java.net.URLDecoder;
import java.util.Arrays;
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
					String[] previewContent = {};
					if (kvs.get("content", hashURL, "page") != null) {
						String content = new String(kvs.get("content", hashURL, "page"));
						content = content.replaceAll("[.,:;!\\?\'\"()-]", " ");
						String[] splitContent = content.split("\\s+");

						int firstLocation = Ranker.urlToPreviewIndex.get(url);
						int start = Math.max(0, firstLocation - 10);
						int end = Math.min(firstLocation + 90, splitContent.length);

						previewContent = Arrays.copyOfRange(splitContent, start, end);
					}
					String preview = "No preview content available ";
					if (previewContent.length > 0){
						preview = String.join(" ", previewContent);
					}
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
