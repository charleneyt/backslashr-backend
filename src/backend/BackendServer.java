package backend;

import static webserver.Server.*;

import java.net.URLDecoder;
import java.util.*;
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
					if (previewContent.length > 0) {
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

		// make it only supports single word search, and purely look at the position
		// count?
		get("/image-search", (req, res) -> {
			// this header is needed to for CORS
			res.header("Access-Control-Allow-Origin", "*");
			String query = URLDecoder.decode(req.queryParams("query").toLowerCase(), "UTF-8");
			System.out.println("Query is: " + query);
			String[] searchTerms = query.split("\\s+");
			JSONObject results = new JSONObject();
			results.put("results", null);

			if (kvs.existsRow("images_output", searchTerms[0])) {
				JSONArray list = new JSONArray();
				String[] urls = new String(kvs.get("images_output", searchTerms[0], "value")).split(",");
				Map<Integer, List<String>> counts = new TreeMap<>(Collections.reverseOrder());
				Set<String> seenUrls = new HashSet<>();
				for (String url : urls) {
					if (url.contains(":")) {
						int lastColon = url.lastIndexOf(":");
						String newUrl = url.substring(0, lastColon);
						String allPosition = url.substring(lastColon);
						String[] positions = allPosition.split(" ");
						if (!seenUrls.contains(newUrl)) {
							if (newUrl.startsWith("http")) {
								if (newUrl.endsWith(".jpg") || newUrl.endsWith(".jpeg") || newUrl.endsWith(".png")) {
									List<String> curr = counts.getOrDefault(positions.length, new ArrayList<>());
									curr.add(newUrl);
									counts.put(positions.length, curr);
									seenUrls.add(newUrl);
								}
							}

						}
					}
				}
				int needed = 10;
				for (int count : counts.keySet()) {
					for (String url : counts.get(count)) {
						JSONObject data = new JSONObject();
						String hashURL = Hasher.hash(url);
						String altText = "No alt text available";
						try {
							altText = new String(kvs.get("images", hashURL, "altText"));
						} catch (Exception e) {
//							System.out.println("Sorry no alt text available");
						}

						data.put("URL", url);
						data.put("altText", altText);
						list.add(data);
						needed--;
						if (needed == 0) {
							break;
						}
					}

				}
				results.put("results", list);
			}

			return results;
		});
	}
}
