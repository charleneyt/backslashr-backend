package backend;

import static webserver.Server.*;

import kvs.KVSClient;
import kvs.Row;
import tools.Hasher;

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
		System.out.println("Backend listening on " + myPort + " ... ");

		get("/search", (req, res) -> {
			// this header is needed to for CORS
			res.header("Access-Control-Allow-Origin", "*");
			String query = req.queryParams("query");
			System.out.println("Query is: " + query);
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
					if (idx < 0) {
						list.add(result);
					} else {
						try {
							// Create JSON Object to add attribute URL and content
							JSONObject data = new JSONObject();
							String url = result.substring(0, idx);
							String hashURL = Hasher.hash(url);
							String content = new String(kvs.get("crawl", hashURL, "page"));
							content = content.replaceAll("[.,:;!\\?\'\"()-]", " ").replaceAll("<[^>]*>", " ");

							StringBuilder previewContent = new StringBuilder();

							String[] splitContent = content.split("\\s+");

							// Add only a small preview of the content
							if (splitContent.length > 100) {
								for (int i = 0; i < 100; i++) {
									previewContent.append(splitContent[i] + " ");
								}
							} else {
								for (int i = 0; i < splitContent.length; i++) {
									previewContent.append(splitContent[i] + " ");
								}
							}

							data.put("URL", url);
							data.put("content", previewContent.toString());
							list.add(data);
						} catch (Exception e) {
							System.out.println("Exception " + e);
						}
					}
				}

				results.put("results", list);

				return results;
			} catch (Exception e) {
				System.out.println("Failed to get row.");
				if (kvs == null) {
					System.out.println("KVS is null!!!");
				}
				return results;
			}

		});
	}

}
