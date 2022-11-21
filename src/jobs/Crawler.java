package jobs;

import flame.*;
import kvs.*;
import tools.*;
import tools.HTTP.*;
import java.net.*;
import java.util.regex.*;

import java.util.*;

public class Crawler {
	static boolean parsedBlackList = false;
	static List<String> blacklist = new ArrayList<>();

	public static void run(FlameContext ctx, String[] args) throws Exception {
		// Check whether the latter contains a single element (the seed URL), and output
		// an error message (using the context’s output method)
		// if it does not. If it does, output a success message, maybe "OK"
		System.out.println("Executing crawler ...");
		if (args.length < 1 || args.length > 2) {
			ctx.output(
					"Invalid Argument! There must have one String for seed URL, and one optional argument for blacklist table name");
			return;
		}

		FlameRDD urlQueue;

		if (args[0].startsWith("http")) {
			String[] parsedSeedUrl = parseURL(args[0]);
			if (parsedSeedUrl[0] == null && parsedSeedUrl[1] == null && parsedSeedUrl[2] == null) {
				if (parsedSeedUrl[3].length() > 0 && !parsedSeedUrl[3].startsWith("/")) {
					// default protocol to http if given link is not a relative one
					parsedSeedUrl[0] = "http";
					parsedSeedUrl[1] = parsedSeedUrl[3];
					parsedSeedUrl[3] = "/";
				} else {
					ctx.output("Invalid seed URL: missing protocol and host name");
					return;
				}
			}

			if (parsedSeedUrl[2] == null || parsedSeedUrl[2].length() == 0) {
				parsedSeedUrl[2] = parsedSeedUrl[0].equals("http") ? "80" : "443";
			}
			if (parsedSeedUrl[3] == null) {
				parsedSeedUrl[3] = "/";
			}

			String seedURL = parsedSeedUrl[0] + "://" + parsedSeedUrl[1] + ":" + parsedSeedUrl[2] + parsedSeedUrl[3];

			// for (String something : normalizeUrl(findUrl("<A HREF=#abc></A> <a ref=lala
			// hRef=\"blah.html#test\"></a><a HREF=\'../blubb/123.html\'> \n<a
			// href=/one/two.html></a><a></a><h1></h1><a
			// href=http://elsewhere.com/some.html></a><a href=/SL7sCi.html>felt</a> <a
			// href=https://foo.com/bar/xyz.html"), "https://foo.com:8000/bar/xyz.html")){
			// System.out.println(something);
			// }

			// if everything's OK, create an initial FlameRDD (perhaps called urlQueue) by
			// parallelizing the seed URL
			urlQueue = ctx.parallelize(Arrays.asList(seedURL));
			// FlameRDD urlQueue = ctx.parallelize(Arrays.asList(args[0]));
		} else {
			urlQueue = ctx.fromTable(args[0], r -> r.get("value"));
		}

		// and then set up a while loop that runs until urlQueue.count() is zero
		while (urlQueue.count() > 0) {
			// Within this loop, replace urlQueue with the result of a flatMap on itself.
			// The flatMap will be running in parallel on all the workers, and each worker
			// will be returning the (new) URLs it wants to put into the queue. In the
			// lambda argument, take the URL, create a HttpURLConnection, set the request
			// method to GET, use setRequestProperty to add the required header, and connect
			// it; then check whether the response code is 200. If so, read the input
			// stream, create a Row whose key is the hash of the URL, add the url and page
			// fields, and use the putRow from KVSClient to upload it to the crawl table in
			// the KVS. For now, return an empty list from the flatMap, so that the loop
			// will terminate after a single iteration.
			urlQueue = urlQueue.flatMap(url -> {
//        KVSClient kvs = FlameContext.getKVS();
				KVSClient kvs = new KVSClient("localhost:8000");

				// process blacklist if given argument is not null
				if (!parsedBlackList && args.length == 2) {
					Iterator<Row> iter = kvs.scan(args[1], null, null);
					if (iter != null) {
						while (iter.hasNext()) {
							Row next = iter.next();
							if (next == null) {
								break;
							}
							blacklist.add(next.get("pattern"));
						}
					}
					parsedBlackList = true;
				}

				String[] parsedUrl = parseURL(url);
				String hostHash = Hasher.hash(parsedUrl[1]);

				boolean robotReq = false;
				Row robotrow = null;
				// otherwise, check if robots.txt is downloaded
				if (kvs.existsRow("hosts", hostHash)) {
					robotrow = kvs.getRow("hosts", hostHash);
					if (robotrow != null && robotrow.columns().contains("robotsReq")) {
						// no need to make robots request again!
						robotReq = true;
					}
				}

				// if there's no robots column in hosts table with current host, make a GET
				// request
				if (!robotReq) {
					String robotsUrl = makeRobotsUrl(parsedUrl);
					Response r = HTTP.doRequest("GET", robotsUrl, null);
					kvs.put("hosts", hostHash, "robotsReq", String.valueOf(r.statusCode()).getBytes());
					if (r.statusCode() == 200 && r.body() != null) {
						kvs.put("hosts", hostHash, "robots", r.body());
						robotReq = true;
					}
				}
				// if robotsReq exists, we already made requests before, no matter what status
				// it returned
				// only when the req returns 200 do we put the body into robots column
				String robotTxt = "";
				if (robotReq && "200".equals(new String(kvs.get("hosts", hostHash, "robotsReq")))) {
					robotTxt = new String(kvs.get("hosts", hostHash, "robots"));
				}

				// initialize rules and return list
				List<String> rules = parseRules(robotTxt);
				List<String> ret = new LinkedList<>();

				// only crawl data if passed the rule check
				if (checkRules(parsedUrl[3], rules)) {
					String normalizedOriginal = normalizeImpl(url, null, new StringBuilder());
					if (normalizedOriginal == null || normalizedOriginal.length() == 0) {
						return ret;
					}
					String urlHash = Hasher.hash(normalizedOriginal);
					// if already crawled (check url matches, and use responseCode as indicator)
					if (kvs.existsRow("crawl", urlHash)) {
						Row r = kvs.getRow("crawl", urlHash);
						if (r.columns().contains("url") && r.get("url").equals(normalizedOriginal)
								&& r.columns().contains("responseCode")) {
							return ret;
						}
					}

					// sending HEAD request
					HttpURLConnection.setFollowRedirects(false);
					HttpURLConnection con = (HttpURLConnection) (new URL(url)).openConnection();
					con.setRequestMethod("HEAD");
					con.setRequestProperty("User-Agent", "cis5550-crawler");
					con.setInstanceFollowRedirects(false); // must set redirects to false!
					con.connect();
					int code = con.getResponseCode();
					String type = con.getContentType();
					int length = con.getContentLength();

					Row tempRow = new Row(urlHash);
					if (url != null) {
						tempRow.put("url", url.getBytes());
					}

					if (code != 200) {
						tempRow.put("responseCode", String.valueOf(code));

						switch (code) {
						case 301:
						case 302:
						case 303:
						case 307:
						case 308:
							String redirect = con.getHeaderField("Location");
							if (redirect != null) {
								redirect = normalizeImpl(URLDecoder.decode(redirect, "UTF-8"), normalizedOriginal,
										new StringBuilder());
								// if we have redirect from HEAD response, save the status code in crawl table
								// and return the url from Location header
								con.disconnect();

								if (redirect != null && redirect.length() != 0) {
									tempRow.put("redirectURL", redirect);
									if (!kvs.existsRow("crawl", urlHash)) {
										kvs.putRow("crawl", tempRow);
									}
									return Arrays.asList(redirect);
								} else {
									if (!kvs.existsRow("crawl", urlHash)) {
										kvs.putRow("crawl", tempRow);
									}
									return ret;
								}
							}
							break;
						}
					}

					if (type != null) {
						tempRow.put("contentType", type.getBytes());
					}
					if (length != -1) {
						tempRow.put("length", String.valueOf(length).getBytes());
					}

					con.disconnect();

					// only issue GET if the HEAD response is 200 and type is text/html
					if (code == 200 && "text/html".equals(type)) {
						// check if host has been called before, and if so, whether that was more than 1
						// second ago
						if (kvs.existsRow("hosts", hostHash)) {
							Row hostRow = kvs.getRow("hosts", hostHash);
							if (hostRow != null && hostRow.columns().contains("time")
									&& Long.valueOf(hostRow.get("time")) + 1000 >= System.currentTimeMillis()) {
								return Arrays.asList(url);
							}
						}
						// update the hosts table's access time if it's last called more than 1 second
						// ago
						kvs.put("hosts", hostHash, "time", String.valueOf(System.currentTimeMillis()).getBytes());

						// new GET connection only if responseCode is 200 and type is text/html
						con = (HttpURLConnection) (new URL(url)).openConnection();
						con.setRequestMethod("GET");
						con.setRequestProperty("User-Agent", "cis5550-crawler");
						con.setDoInput(true);
						con.connect();
						code = con.getResponseCode();
						tempRow.put("responseCode", String.valueOf(code).getBytes());

						byte[] response = con.getInputStream().readAllBytes();
						if (response != null) {
							// EC 1 check if the content is duplicate with other crawled pages
							// use a table "ec1", where hash value of the String of content is row key, hash
							// value of url is column key, and url is value
							String responseStr = new String(response);
							String responseHash = Hasher.hash(responseStr);
							boolean isDuplicate = false;
							if (kvs.existsRow("ec1", responseHash)) {
								Row contentRow = kvs.getRow("ec1", responseHash);
								for (String contentUrl : contentRow.columns()) {
									String contentUrlHash = contentRow.get(contentUrl);
									byte[] otherContent = kvs.get("crawl", contentUrlHash, "page");
									if (contentUrl != null && otherContent != null
											&& Arrays.equals(otherContent, response)) {
										tempRow.put("canonicalURL", contentUrl.getBytes());
										isDuplicate = true;
										break;
									}
								}
							}

							if (!isDuplicate && response != null) {
								tempRow.put("page", response); // only add "page" if the current page is not a
																// duplicate!
								kvs.put("ec1", responseHash, url, urlHash.getBytes()); // if not a duplicate, save the
																						// current url's content hash to
																						// ec1 table
								for (String newUrl : findUrl(kvs, responseStr, normalizedOriginal, rules)) {
									ret.add(newUrl);
								}
							}
						}
						con.disconnect();
					}

					if (!kvs.existsRow("crawl", urlHash)) {
						kvs.putRow("crawl", tempRow);
					}
				}
				// System.out.println(ret);
				return ret;
			});
//			System.out.println("Finished a round");
//			System.out.println("New table name: " + urlQueue.getTableName());
//			System.out.println("New table count: " + urlQueue.count());
			Thread.sleep(1000);
		}
		// Iterator<Row> iter = FlameContext.getKVS().scan("anchorEC", null, null);
		// if (iter != null){
		// while (iter.hasNext()){
		// Row row = iter.next();
		// if (row == null){
		// break;
		// }
		// // if (FlameContext.getKVS().existsRow("crawl", row.key())){
		// for (String colName : row.columns()) {
		// FlameContext.getKVS().put("crawl", Hasher.hash(row.key()), colName,
		// row.get(colName));
		// }
		// // }
		// }
		// }
		ctx.output("OK");
	}

	static List<String> findUrl(KVSClient kvs, String s, String originalUrl, List<String> rules) throws Exception {
		List<String> urls = new LinkedList<>();
		Matcher m = Pattern.compile(
				"<[Aa][\\s\\S&&[^>]]*[Hh][Rr][Ee][Ff]=[\"\']?([\\S&&[^\"\'<>]]+)[\"\']?[\\s\\S&&[^>]]*>([\\s\\S&&[^<]]*)</[Aa]>")
				.matcher(s);
		StringBuilder sb = new StringBuilder();
		Map<String, String> seen = new HashMap<>();
		while (m.find()) {
			if (m.group(1) == null || m.group(1).length() == 0) {
				continue;
			}
			String url = m.group(1);
			boolean unwantedType = false;
			int dot = url.lastIndexOf(".");
			if (dot != -1 && (dot == url.length() - 4 || dot == url.length() - 5)) {
				switch (url.substring(dot).toLowerCase()) {
				case ".jpg":
				case ".jpeg":
				case ".gif":
				case ".png":
				case ".txt":
					unwantedType = true;
					break;
				}
			}
			if (unwantedType) {
				continue;
			}
			int sharpIdx = url.indexOf("#");
			if (sharpIdx != -1) {
				url = url.substring(0, sharpIdx);
				if (url.length() == 0) {
					continue;
				}
			}
			String copy = originalUrl;
			String normalizedUrl = normalizeImpl(url, copy, sb);
			if (normalizedUrl == null || normalizedUrl.length() == 0) {
				continue;
			}

			// EC 2 if there's a blacklist, new url needs to be not matching with any given
			// pattern
			if (checkRules(parseURL(normalizedUrl)[3], rules) && notBlacklisted(normalizedUrl, blacklist)) {
				urls.add(normalizedUrl);

				// EC 3 anchor text extraction
				if (m.group(2) != null && m.group(2).length() > 0) {
					String anchorTxt = m.group(2);

					// per Ed: don't need to eliminate duplicates - just concatenate the text from
					// different anchors, with a space in between
					if (seen.containsKey(normalizedUrl)) {
						seen.put(normalizedUrl, seen.get(normalizedUrl) + " " + anchorTxt);
					} else {
						seen.put(normalizedUrl, anchorTxt);
					}

					// if (kvs.existsRow("crawl", rowHash)){
					// Row currRow = kvs.getRow("crawl", rowHash);
					// if (currRow.columns().contains("anchor-" + normalizedUrl)){
					// String existingAnchor = currRow.get(normalizedUrl);
					// if (existingAnchor != null && existingAnchor.length() > 0){
					// for (String a : existingAnchor.split(" ")){
					// if (a.equals(anchorTxt)){
					// anchorExists = true;
					// break;
					// }
					// }
					// }
					// if (!anchorExists){
					// anchorSb.append(existingAnchor);
					// }
					// }
					// }

					// if (!anchorExists){
					// if (anchorSb.length() > 0){
					// anchorSb.append(" " + anchorTxt);
					// } else {
					// anchorSb.append(anchorTxt);
					// }
					// // put the anchor text in crawl table under the found link, with column name
					// anchor-url
					// kvs.put("crawl", rowHash, "anchor-" + normalizedUrl,
					// anchorSb.toString().getBytes());
					// // if the crawl table has url now, update the anchor text
					// // if (kvs.existsRow("crawl", rowHash)){
					// // System.out.println("putting anchor to crawl " + originalUrl);
					// // kvs.put("crawl", rowHash, "anchor", anchorSb.toString().getBytes());
					// // }
					// }
				}
			}
		}
		for (String url : seen.keySet()) {
			kvs.put("anchorEC", url, "anchor-" + originalUrl, seen.get(url).getBytes());
		}
		return urls;
	}

	static String normalizeImpl(String url, String originalUrl, StringBuilder sb) {
		sb.setLength(0);
		if (url == null || url.length() == 0) {
			return null;
		}

		if (url.startsWith("http://")) {
			sb.append(url.substring(0, 7));
			url = url.substring(7);
			int col = url.indexOf(":");
			if (col != -1 && col + 1 < url.length() && Character.isDigit(url.charAt(col + 1))) {
				sb.append(url);
			} else {
				if (url.contains("/")) {
					int slash = url.indexOf("/");
					sb.append(url.substring(0, slash));
					sb.append(":80");
					sb.append(url.substring(slash));
				} else {
					sb.append(url + ":80");
				}
			}
		} else if (url.startsWith("https://")) {
			sb.append(url.substring(0, 7));
			url = url.substring(8);
			int col = url.indexOf(":");
			if (col != -1 && col + 1 < url.length() && Character.isDigit(url.charAt(col + 1))) {
				sb.append(url);
			} else {
				if (url.contains("/")) {
					int slash = url.indexOf("/");
					sb.append(url.substring(0, slash));
					sb.append(":443");
					sb.append(url.substring(slash));
				} else {
					sb.append(url + ":443");
				}
			}
		} else if (!url.contains("://")) {
			if (originalUrl.startsWith("http://")) {
				sb.append(originalUrl.substring(0, 7));
				originalUrl = originalUrl.substring(7);
			} else {
				sb.append(originalUrl.substring(0, 8));
				originalUrl = originalUrl.substring(8);
			}
			if (url.startsWith("/")) {
				sb.append(originalUrl.substring(0, originalUrl.indexOf("/")));
				sb.append(url);
			} else {
				int lastSlash = originalUrl.lastIndexOf("/");
				if (lastSlash == -1) {
					return null;
				}
				originalUrl = originalUrl.substring(0, lastSlash);

				while (url.startsWith("../")) {
					lastSlash = originalUrl.lastIndexOf("/");
					if (lastSlash == -1) {
						return null;
					}
					if (url.length() == 3) {
						url = url.substring(2);
					} else {
						url = url.substring(3);
					}
					originalUrl = originalUrl.substring(0, lastSlash);
				}
				if ("..".equals(url)) {
					return null;
				}

				sb.append(originalUrl);
				if (url.startsWith("/")) {
					sb.append(url);
				} else {
					sb.append("/" + url);
				}
			}
		}
		return sb.toString();
	}

	static String[] parseURL(String url) {
		String result[] = new String[4];
		int slashslash = url.indexOf("//");
		if (slashslash > 0) {
			result[0] = url.substring(0, slashslash - 1);
			int nextslash = url.indexOf('/', slashslash + 2);
			if (nextslash >= 0) {
				result[1] = url.substring(slashslash + 2, nextslash);
				result[3] = url.substring(nextslash);
			} else {
				result[1] = url.substring(slashslash + 2);
				result[3] = "/";
			}
			int colonPos = result[1].indexOf(':');
			if (colonPos > 0) {
				result[2] = result[1].substring(colonPos + 1);
				result[1] = result[1].substring(0, colonPos);
			}
		} else {
			if (url == null || url.length() == 0) {
				result[3] = "/";
			} else {
				result[3] = url;
			}
		}

		return result;
	}

	static List<String> parseRules(String robot) {
		if (robot.length() == 0) {
			return new LinkedList<>();
		}
		String[] splitRobotTxt = robot.split("\n\n");

		List<String> self = new LinkedList<>();
		List<String> wildcard = new LinkedList<>();
		boolean selfExists = false;
		int idx = 0;
		while (idx < splitRobotTxt.length) {
			if (splitRobotTxt[idx].startsWith("User-agent: cis5550-crawler\n")) {
				Collections.addAll(self, splitRobotTxt[idx].split("\n"));
				self.remove(0);
				selfExists = true;
				break;
			} else if (splitRobotTxt[idx].startsWith("User-agent: *\n")) {
				Collections.addAll(wildcard, splitRobotTxt[idx].split("\n"));
				wildcard.remove(0);
			}
			idx++;
		}
		if (selfExists) {
			return self;
		} else {
			return wildcard;
		}
	}

	static boolean checkRules(String url, List<String> rules) {
		if (url == null || url.length() == 0) {
			return false;
		}
		for (String rule : rules) {
			if (rule.startsWith("Allow: ")) {
				if (url.startsWith(rule.substring(7))) {
					break;
				}
			} else if (rule.startsWith("Disallow: ")) {
				if (url.startsWith(rule.substring(10))) {
					return false;
				}
			}
		}
		return true;
	}

	static String makeRobotsUrl(String[] parsed) {
		StringBuilder sb = new StringBuilder();
		if (parsed[0] != null && parsed[0].length() > 0) {
			sb.append(parsed[0] + "://");
		}
		sb.append(parsed[1]);
		if (parsed[2] != null && parsed[2].length() > 0) {
			sb.append(":" + parsed[2]);
		}
		sb.append("/robots.txt");
		return sb.toString();
	}

	static boolean notBlacklisted(String url, List<String> blacklist) {
		for (String bl : blacklist) {
			bl = bl.replace("*", "\\S*");
			if (Pattern.matches(bl, url)) {
				// we have a match! should ignore the given url
				return false;
			}
		}
		return true;
	}
}