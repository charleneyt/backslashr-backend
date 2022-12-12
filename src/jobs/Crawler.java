package jobs;

import flame.*;
import kvs.*;
import tools.*;

import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.util.regex.*;

import java.util.*;

public class Crawler {
	static boolean parsedBlackList = false;
	static List<String> blacklist = new ArrayList<>();
	static boolean restartLog1 = true;
	static boolean restartLog2 = true;
	final static Set<String> allowedSuffix = new HashSet<>(Arrays.asList("com", "net", "org", "edu", "gov"));
	public final static Set<String> authorityHubs = new HashSet<>(Arrays.asList("wikipedia.com", "baeldung.com", "w3schools.com", "geeksforgeeks.org"));
	static boolean debugMode = false;

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
		
		FileWriter fw3 = new FileWriter("./crawler_log_outside_lambda", true);
		fw3.write("Starting a round, time at: " + System.currentTimeMillis() + "\n");
		fw3.write("Starting table name: " + urlQueue.getTableName() + "\n");
		fw3.close();

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
				System.out.println("working on " + url + " , at time: " + System.currentTimeMillis() + "\n");
				if (debugMode) {
					FileWriter fw = new FileWriter("./crawler_log_inside_lambda", true);
					fw.write("working on " + url + " , at time: " + System.currentTimeMillis() + "\n");
					fw.close();					
				}

				if (FlameContext.getKVS() == null) {
					FlameContext.setKVS("localhost:8000");
				}
				KVSClient kvs = FlameContext.getKVS();
				// KVSClient kvs = new KVSClient("localhost:8000");

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
				String hostHash = parsedUrl[1];

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
					HttpURLConnection.setFollowRedirects(false);
					HttpURLConnection con = (HttpURLConnection) (new URL(robotsUrl)).openConnection();

					con.setRequestMethod("GET");
					con.setDoInput(true);
					con.setRequestProperty("User-Agent", "cis5550-crawler");
					con.setInstanceFollowRedirects(false); // must set redirects to false!
					con.setConnectTimeout(1000);
					// Update where to put timeout Cindy 12/02
					try {
						con.connect();
					} catch (Exception e) {
						System.out.println("Robot failed to connect");
						return Arrays.asList(new String[] {});
					}

					int robotsCode = con.getResponseCode();

//          Response r = HTTP.doRequest("GET", robotsUrl, null);
					kvs.put("hosts", hostHash, "robotsReq", String.valueOf(robotsCode).getBytes());
					if (robotsCode == 200) {
						byte[] robotsResponse = con.getInputStream().readAllBytes();
						if (robotsResponse != null) {
							kvs.put("hosts", hostHash, "robots", robotsResponse);
							robotReq = true;
						}
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
				if (checkRules(parsedUrl[3], rules) && notBlacklisted(url, blacklist)) {
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
					con.setConnectTimeout(5000);
					// Update where to put timeout Cindy 12/02
					try {
						con.connect();
					} catch (Exception e) {
						System.out.println("Head failed to connect");
						return Arrays.asList(new String[] {});
					}

					int code = con.getResponseCode();
					String type = con.getContentType();
					int length = con.getContentLength();

					if (url != null) {
						kvs.put("crawl", urlHash, "url", url.getBytes());
						// tempRow.put("url", url.getBytes());
					}

					if (code != 200) {
						kvs.put("crawl", urlHash, "responseCode", String.valueOf(code));
						// tempRow.put("responseCode", String.valueOf(code));

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
									kvs.put("crawl", urlHash, "redirectURL", redirect);
									// tempRow.put("redirectURL", redirect);
									// if (!kvs.existsRow("crawl", urlHash)) {
									// kvs.putRow("crawl", tempRow);
									// }
									return Arrays.asList(redirect);
								} else {
									// if (!kvs.existsRow("crawl", urlHash)) {
									// kvs.putRow("crawl", tempRow);
									// }
									return ret;
								}
							}
							break;
						}
					}

					if (type != null) {
						kvs.put("crawl", urlHash, "contentType", type);
						// tempRow.put("contentType", type.getBytes());
					}
					if (length != -1) {
						kvs.put("crawl", urlHash, "length", String.valueOf(length));
						// tempRow.put("length", String.valueOf(length).getBytes());
					}
					// if (!kvs.existsRow("crawl", urlHash)) {
					// kvs.putRow("crawl", tempRow);
					// }

					con.disconnect();

					// only issue GET if the HEAD response is 200 and type is text/html
					if (code == 200 && type != null && type.contains("text/html")) {
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
						System.out.println("crawling " + url + ", at time: " + System.currentTimeMillis() + "\n");
						if (debugMode) {
							FileWriter fw = new FileWriter("./crawler_log_inside_lambda", true);
							fw.write("crawling " + url + ", at time: " + System.currentTimeMillis() + "\n");
							fw.close();					
						}
						
						con = (HttpURLConnection) (new URL(url)).openConnection();

						con.setRequestMethod("GET");
						con.setRequestProperty("User-Agent", "cis5550-crawler");
						con.setDoInput(true);
						con.setConnectTimeout(5000);
						// Update where to put timeout Cindy 12/02
						try {
							con.connect();
						} catch (Exception e) {
							System.out.println("Code 200 failed to connect");
							return Arrays.asList(new String[] {});
						}
						code = con.getResponseCode();
						// tempRow.put("responseCode", String.valueOf(code).getBytes());
						kvs.put("crawl", urlHash, "responseCode", String.valueOf(code));

						// increment the crawler count by the root domain
						// for example, sports.cnn.com/a.html would be cnn.com
						// this is used to monitor the crawler status and prevent the results from too
						// dense
						int lastDot = parsedUrl[1].lastIndexOf(".");
						if (lastDot != -1) {
							int startIdx = parsedUrl[1].substring(0, lastDot).lastIndexOf(".") + 1;
							String domainName = parsedUrl[1].substring(startIdx);
							if (domainName.length() > 0) {
								String domainHash = Hasher.hash(domainName);
								long crawlCount = 0;
								if (kvs.existsRow("domain", domainHash)) {
									Row domainRow = kvs.getRow("domain", domainHash);
									crawlCount = Long.valueOf(domainRow.get("count"));
									if (authorityHubs.contains(domainName)) {
										if (crawlCount > 10000) {
											return Arrays.asList(new String[] {});
										}
									} else if (crawlCount > 200) {
										return Arrays.asList(new String[] {});
									}
									// else Arrays.asList(new String[] {});
								}
								kvs.put("domain", domainHash, "count", String.valueOf(crawlCount + 1));
								if (crawlCount == 0) {
									kvs.put("domain", domainHash, "domain", domainName);
								}
							}
						} else {
							String domainName = parsedUrl[1];
							if (domainName.length() > 0) {
								String domainHash = Hasher.hash(domainName);
								long crawlCount = 0;
								if (kvs.existsRow("domain", domainHash)) {
									Row domainRow = kvs.getRow("domain", domainHash);
									crawlCount = Long.parseLong(domainRow.get("count"));
									if (crawlCount > 1000) {
										return Arrays.asList(new String[] {});
									}
								}
								kvs.put("domain", domainHash, "count", String.valueOf(crawlCount + 1));
								if (crawlCount == 0) {
									kvs.put("domain", domainHash, "domain", domainName);
								}
							}
						}

						byte[] response = con.getInputStream().readAllBytes();
						if (response != null) {
							// EC 1 check if the content is duplicate with other crawled pages
							// use a table "canonicalURL", where hash value of the String of content is row
							// key, hash
							// value of url is column key, and url is value
							String responseStr = new String(response);
							// filter out style and script tags and its content
							String processed = responseStr
									.replaceAll("(<style.*?>.*?</style.*?>)|(<script.*?>[\\s\\S]*?</script.*?>)", "");
							for (String newUrl : findUrl(kvs, processed, normalizedOriginal, rules)) {
								ret.add(newUrl);
							}

							processed = processed.replaceAll("<[^>]*>", "");

							// saving page content to new table
							kvs.put("content", urlHash, "url", url);
							kvs.put("content", urlHash, "page", processed);
						}
						con.disconnect();
					}
				}
				return ret;
			});
			// logging crawling round info
			System.out.println("Finished a round");
			System.out.println("New table name: " + urlQueue.getTableName());
			System.out.println("New table count: " + urlQueue.count());
			
			FileWriter fw2 = new FileWriter("./crawler_log_outside_lambda", true);
			fw2.write("Finished a round, time at: " + System.currentTimeMillis() + "\n");
			fw2.write("New table name: " + urlQueue.getTableName() + "\n");
			fw2.write("New table count: " + urlQueue.count() + "\n");
			fw2.close();				

//			Thread.sleep(1000);
		}

		ctx.output("OK");
	}

	static List<String> findUrl(KVSClient kvs, String s, String originalUrl, List<String> rules) throws Exception {
		Set<String> urlSet = new HashSet<>();
		Map<String, String> imgMap = new HashMap<>();
		Matcher m = Pattern.compile(
				"<[Aa][\\s\\S&&[^>]]*[Hh][Rr][Ee][Ff]=[\"\']?([\\S&&[^\"\'<>]]+)[\"\']?[\\s\\S&&[^>]]*>([\\s\\S&&[^<]]*)</[Aa]>|<[Ii][Mm][Gg][\\s\\S&&[^>]]*[Ss][Rr][Cc]=[\"\']?([\\S&&[^\"\'<>]]+)[\"\']?[\\s\\S&&[^>]]*>")
				.matcher(s);
		StringBuilder sb = new StringBuilder();
		Map<String, String> seen = new HashMap<>();
		while (m.find()) {
			if ((m.group(1) == null || m.group(1).length() == 0) && (m.group(3) == null || m.group(3).length() == 0)) {
				continue;
			}
			boolean img = m.group(1) == null || m.group(1).length() == 0;
			String url = "";
			String altText = "";
			if (!img) {
				url = m.group(1);
				// check the extension for normal url found
				boolean unwantedType = false;
				int dot = url.lastIndexOf(".");
				if (dot != -1 && (dot == url.length() - 4 || dot == url.length() - 5)) {
					switch (url.substring(dot).toLowerCase()) {
					case ".jpg":
					case ".jpeg":
					case ".gif":
					case ".png":
					case ".txt":
					case ".js":
					case ".pdf":
						unwantedType = true;
						break;
					}
				}
				if (unwantedType) {
					continue;
				}
			} else {
				// url is for image crawling task
				url = m.group(3);
				// if the tages contain alt, save that string as well
				if (m.group() != null && m.group().toLowerCase().contains("alt=\"")) {
					int idx = m.group().toLowerCase().indexOf("alt=\"");
					if (idx != -1) {
						String wholeTag = m.group().substring(idx + 5);
						int nextQuote = wholeTag.indexOf("\"");
						if (nextQuote != -1) {
							altText = wholeTag.substring(0, nextQuote);
						}
					}
				}
			}

			// check the url after trimming off # parts
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
				if (img) {
					String currAltText = imgMap.getOrDefault(normalizedUrl, "");
					if (currAltText.length() > 0) {
						currAltText += "," + altText;
					} else {
						currAltText = altText;
					}
					imgMap.put(normalizedUrl, currAltText);
				} else {
					urlSet.add(normalizedUrl);
				}

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
				}
			}
		}

		// Temporary exclude Cindy 12/02
//		for (String url : seen.keySet()) {
//			String key = Hasher.hash(url);
//			if (!kvs.existsRow("anchorEC", key)) {
//				kvs.put("anchorEC", key, "url", url.getBytes());
//			}
//			kvs.put("anchorEC", key, "anchor-" + originalUrl, seen.get(url).getBytes());
//		}

		// save the outdegrees of current url to outdegrees table (value is comma
		// separated, to be used in pageranks)
		sb.setLength(0);
		List<String> urlToVisit = new LinkedList<>();
		for (String url : urlSet) {
			if (sb.length() > 0) {
				sb.append(",");
			}
			sb.append(url);

			// check if already crawled, if so, don't add to queue
			if (!kvs.existsRow("crawl", Hasher.hash(url))) {
				// check the count by domain name, pass if currently crawled enough
				String[] parsedUrl = parseURL(url);
				int lastDot = parsedUrl[1].lastIndexOf(".");
				if (lastDot != -1) {
					int startIdx = parsedUrl[1].substring(0, lastDot).lastIndexOf(".") + 1;
					String domainName = parsedUrl[1].substring(startIdx);
					if (domainName.length() > 0 && (lastDot = domainName.lastIndexOf(".")) != domainName.length() - 1
							&& allowedSuffix.contains(domainName.substring(lastDot + 1))) {
						String domainHash = Hasher.hash(domainName);
						if (kvs.existsRow("domain", domainHash)) {
							Row domainRow = kvs.getRow("domain", domainHash);
							long crawlCount = Long.valueOf(domainRow.get("count"));
							if (authorityHubs.contains(domainName)) {
								if (crawlCount > 10000) {
									continue;
								}
							} else if (crawlCount > 200) {
								continue;
							}
							// else Arrays.asList(new String[] {});
						}
						// check if already crawled, if so, don't add to queue
						urlToVisit.add(url);
					} else {
						continue;
					}
				} else {
					continue;
				}
			}
		}
		// add to outdegrees table for current link and its outgoing links
		String hashkey = Hasher.hash(originalUrl);
		kvs.put("outdegrees", hashkey, "url", originalUrl);
		kvs.put("outdegrees", hashkey, "links", sb.toString());

		// save the images crawled (with potential alt text) to images table
		sb.setLength(0);
		for (String url : imgMap.keySet()) {
			if (kvs.existsRow("images", url)) {
				Row r = kvs.getRow("images", url);
				if (r.columns().contains("altText")) {
					sb.append(r.get("altText"));
				}
			}
			if (sb.length() > 0) {
				sb.append(",");
			}
			sb.append(imgMap.get(url));
			String key = Hasher.hash(url);
			kvs.put("images", key, "url", url);
			kvs.put("images", key, "altText", sb.toString());
		}
		return urlToVisit;
	}

	static String normalizeImpl(String url, String originalUrl, StringBuilder sb) throws IOException {
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
					if (url.substring(slash).contains("/")) {
						sb.append(url.substring(slash));
					} else {
						sb.append(url.substring(slash) + "/");
					}
				} else {
					sb.append(url + ":80/");
				}
			}
		} else if (url.startsWith("https://")) {
			sb.append(url.substring(0, 8));
			url = url.substring(8);
			int col = url.indexOf(":");
			if (col != -1 && col + 1 < url.length() && Character.isDigit(url.charAt(col + 1))) {
				sb.append(url);
			} else {
				if (url.contains("/")) {
					int slash = url.indexOf("/");
					sb.append(url.substring(0, slash));
					sb.append(":443");
					if (url.substring(slash).contains("/")) {
						sb.append(url.substring(slash));
					} else {
						sb.append(url.substring(slash) + "/");
					}
				} else {
					sb.append(url + ":443/");
				}
			}
		}
		// For URL that is already a link, don't append to the original URL
		else if (url.contains("//")) {
			if (url.contains("?")) {
				String[] splitURL = url.split("\\?");
				// Lily 12/5/22
				if (originalUrl != null && originalUrl.startsWith("http://")) {
					sb.append(originalUrl.substring(0, 5));

					if (splitURL[0].endsWith("/")) {
						sb.append(splitURL[0].substring(0, splitURL[0].length() - 1));
						sb.append(":80/");
					} else {
						sb.append(splitURL[0]);
						sb.append(":80?");
					}
					for (int i = 1; i < splitURL.length; i++) {
						sb.append(splitURL[i]);
					}

				} else if (originalUrl != null && originalUrl.startsWith("https://")) {
					sb.append(originalUrl.substring(0, 6));
					if (splitURL[0].endsWith("/")) {
						sb.append(splitURL[0].substring(0, splitURL[0].length() - 1));
						sb.append(":443/");
					} else {
						sb.append(splitURL[0]);
						sb.append(":443?");
					}
					for (int i = 1; i < splitURL.length; i++) {
						sb.append(splitURL[i]);
					}
				}

			} else {
				if (originalUrl != null) {
					if (originalUrl.startsWith("http://")) {
						sb.append(originalUrl.substring(0, 5));
					} else {
						sb.append(originalUrl.substring(0, 6));
					}
				}
				sb.append(url);
			}
		} else if (!url.contains("://")) {
			if (originalUrl.startsWith("http://")) {
				sb.append(originalUrl.substring(0, 7));
				originalUrl = originalUrl.substring(7);
			} else {
				sb.append(originalUrl.substring(0, 8));
				originalUrl = originalUrl.substring(8);
			}
			if (!originalUrl.contains("/")) {
				originalUrl += "/";
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
			if (bl != null){
				bl = bl.replace("*", "\\S*");
				if (Pattern.matches(bl, url)) {
					// we have a match! should ignore the given url
					return false;
				}
			}
		}
		return true;
	}
}