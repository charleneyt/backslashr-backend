package jobs;

import flame.*;
import kvs.*;
import tools.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.util.regex.*;

import java.util.*;

public class Crawler {
	static boolean parsedBlackList = false;
	static List<String> blacklist = new ArrayList<>();
	static boolean restartLog1 = true;
	static boolean restartLog2 = true;

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
		if (restartLog1) {
			File file = new File("./crawler_log_inside_lambda");
			Files.deleteIfExists(file.toPath());
			restartLog1 = false;
		}

		if (restartLog2) {
			File file = new File("./crawler_log_outside_lambda");
			Files.deleteIfExists(file.toPath());
			restartLog2 = false;
		}

		FileWriter fw2 = new FileWriter("./crawler_log_outside_lambda", true);

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

		// adding a count for table being processed for current round (used to save page
		// content)
		long countRounds = 0;

		// and then set up a while loop that runs until urlQueue.count() is zero
		while (urlQueue.count() > 0) {
			String currentRoundTable = String.valueOf(countRounds);
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
				System.out.println("Crawling: " + url);
				FileWriter fw = new FileWriter("./crawler_log_inside_lambda", true);
				fw.write("crawling: " + url + "\n");
				fw.flush();
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
					try {
						con.connect();
					} catch (Exception e) {
						e.printStackTrace();
						return null;
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
					try {
						con.connect();
					} catch (Exception e) {
						e.printStackTrace();
						return null;
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

						// new GET connection only if responseCode is 200 and type is text/html
						con = (HttpURLConnection) (new URL(url)).openConnection();
						con.setRequestMethod("GET");
						con.setRequestProperty("User-Agent", "cis5550-crawler");
						con.setDoInput(true);
						try {
							con.connect();
						} catch (Exception e) {
							e.printStackTrace();
							return null;
						}
						code = con.getResponseCode();
						// tempRow.put("responseCode", String.valueOf(code).getBytes());
						kvs.put("crawl", urlHash, "responseCode", String.valueOf(code));

						byte[] response = con.getInputStream().readAllBytes();
						if (response != null) {
							// EC 1 check if the content is duplicate with other crawled pages
							// use a table "canonicalURL", where hash value of the String of content is row
							// key, hash
							// value of url is column key, and url is value
							String responseStr = new String(response);
							kvs.put(currentRoundTable, urlHash, "url", url.getBytes());
							kvs.put(currentRoundTable, urlHash, "page", response);
							fw.write("Downloaded page for " + url + "\n");
							fw.flush();
							for (String newUrl : findUrl(kvs, responseStr, normalizedOriginal, rules)) {
								ret.add(newUrl);
							}

//				String responseHash = Hasher.hash(responseStr);
//				boolean isDuplicate = false;
//				int colCount = 0;
//				if (kvs.existsRow("canonicalURL", responseHash)) {
//					Row contentRow = kvs.getRow("canonicalURL", responseHash);
//					for (String contentUrlHash : contentRow.columns()) {
//						colCount++;
//						String contentUrl = contentRow.get(contentUrlHash);
//						if (contentUrl != null && contentUrl.length() > 0){
//							byte[] otherContent = kvs.get("crawl", Hasher.hash(contentUrl), "page");
//							if (otherContent != null && Arrays.equals(otherContent, response)) {
//								kvs.put("crawl", urlHash, "canonicalURL", contentUrl);
//								// tempRow.put("canonicalURL", contentUrl.getBytes());
//								isDuplicate = true;
//								break;
//							}
//						}
//					}
//				}
//
//				if (!isDuplicate && response != null) {
//					kvs.put("crawl", urlHash, "page", response);
//					// tempRow.put("page", response); // only add "page" if the current page is not duplicate!
//					// if not a duplicate, save the current url's content hash to ec1 table
//					kvs.put("canonicalURL", responseHash, String.valueOf(colCount), url.getBytes()); 
//					for (String newUrl : findUrl(kvs, responseStr, normalizedOriginal, rules)) {
//						ret.add(newUrl);
//					}
//				}
						}
						con.disconnect();
					}

					// if (!kvs.existsRow("crawl", urlHash)) {
					// kvs.putRow("crawl", tempRow);
					// }
				}
				// System.out.println(ret);
				return ret;
			});
			// logging crawling round info
			System.out.println("Finished a round");
			System.out.println("New table name: " + urlQueue.getTableName());
			System.out.println("New table count: " + urlQueue.count());
			fw2.write("Finished a round\n");
			fw2.write("New table name: " + urlQueue.getTableName() + "\n");
			fw2.write("New table count: " + urlQueue.count() + "\n");
			fw2.flush();

			// trigger the indexer work here for the current round's table (only those with
			// page)
			FlamePairRDD imm = ctx.fromTable(currentRoundTable, r -> {
				if (r.get("page") != null && r.get("page").length() > 0) {
					return r.get("url") + "," + r.get("page");
				} else
					return "";
			})
					// then split the String into FlamePair
					.mapToPair(s -> {
						if (s.length() > 0) {
							int idx = s.indexOf(",");
							return new FlamePair(s.substring(0, idx), s.substring(idx + 1));
						} else {
							return new FlamePair("", "");
						}
					});
			// use flatMapToPair to create Iterable<FlamePair>

			FlamePairRDD urlsTable = imm.flatMapToPair(pair -> {
				KVSClient kvs = new KVSClient("localhost:8000");
				List<FlamePair> ret = new ArrayList<FlamePair>();
				if (!"".equals(pair._1())) {
					String url = pair._1();
					String content = pair._2().replaceAll("[.,:;!\\?\'\"()-]", " ").replaceAll("<[^>]*>", "")
							.toLowerCase();
					String[] words = content.split("\\s+");

					// example entry: (word, index1 index2 index3)
					Map<String, String> original = new HashMap<>();
					Map<String, String> stem = new HashMap<>();
					for (int i = 0; i < words.length; i++) {
						String word = words[i];
						if (word != null && word.length() > 0) {
							if (!original.containsKey(word)) {
								ret.add(new FlamePair(word, url));
								original.put(word, String.valueOf(i + 1));
							} else {
								original.put(word, original.get(word) + " " + (i + 1));
							}

							// EC 3 add support for stemming
							Stemmer stemmer = new Stemmer();
							stemmer.add(word.toCharArray(), word.length());
							stemmer.stem();
							String stemWord = stemmer.toString();
							if (stemWord != null && stemWord.length() > 0) {
								if (!stem.containsKey(stemWord)) {
									if (!stemWord.equals(word)) {
										ret.add(new FlamePair(stemWord, url));
									}
									stem.put(stemWord, String.valueOf(i + 1));
								} else {
									stem.put(stemWord, stem.get(stemWord) + " " + (i + 1));
								}
							}
						}
					}
					for (String word : stem.keySet()) {
						// ec1: word, url, index1 index2 index3
						kvs.put("words", word, url, stem.get(word).getBytes());
					}
					for (String word : original.keySet()) {
						if (!stem.containsKey(word)) {
							// ec1: word, url, index1 index2 index3
							kvs.put("words", word, url, original.get(word).getBytes());
						}
					}
				}
				return null;
			});

			// delete the currentRoundTable after indexer work
			new KVSClient("localhost:8000").delete(currentRoundTable);

			// increment the round
			countRounds++;
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
		for (String url : seen.keySet()) {
			kvs.put("anchorEC", url, "anchor-" + originalUrl, seen.get(url).getBytes());
		}

		// save the outdegrees of current url to outdegrees table (value is comma
		// separated, to be used in pageranks)
		sb.setLength(0);
		for (String url : urlSet) {
			if (sb.length() > 0) {
				sb.append(",");
			}
			sb.append(url);
		}
		kvs.put("outdegrees", originalUrl, "value", sb.toString());

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
			kvs.put("images", url, "altText", sb.toString());
		}
		return new ArrayList<>(urlSet);
	}

	static String normalizeImpl(String url, String originalUrl, StringBuilder sb) throws IOException {
		sb.setLength(0);
		if (url == null || url.length() == 0) {
			return null;
		}

		FileWriter fw = new FileWriter("./url_log", true);
		fw.write("URL = " + url + "\n");

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
				if (originalUrl.startsWith("http://")) {
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

				} else {
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
				if (originalUrl.startsWith("http://")) {
					sb.append(originalUrl.substring(0, 5));
				} else {
					sb.append(originalUrl.substring(0, 6));
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

		fw.write("FINAL URL = " + sb.toString() + "\n");
		fw.flush();
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

	public static class Stemmer {
		private char[] b;
		private int i, /* offset into b */
				i_end, /* offset to end of stemmed word */
				j, k;
		private static final int INC = 50;

		/* unit of size whereby b is increased */
		public Stemmer() {
			b = new char[INC];
			i = 0;
			i_end = 0;
		}

		/**
		 * Add a character to the word being stemmed. When you are finished adding
		 * characters, you can call stem(void) to stem the word.
		 */

		public void add(char ch) {
			if (i == b.length) {
				char[] new_b = new char[i + INC];
				for (int c = 0; c < i; c++)
					new_b[c] = b[c];
				b = new_b;
			}
			b[i++] = ch;
		}

		/**
		 * Adds wLen characters to the word being stemmed contained in a portion of a
		 * char[] array. This is like repeated calls of add(char ch), but faster.
		 */

		public void add(char[] w, int wLen) {
			if (i + wLen >= b.length) {
				char[] new_b = new char[i + wLen + INC];
				for (int c = 0; c < i; c++)
					new_b[c] = b[c];
				b = new_b;
			}
			for (int c = 0; c < wLen; c++)
				b[i++] = w[c];
		}

		/**
		 * After a word has been stemmed, it can be retrieved by toString(), or a
		 * reference to the internal buffer can be retrieved by getResultBuffer and
		 * getResultLength (which is generally more efficient.)
		 */
		public String toString() {
			return new String(b, 0, i_end);
		}

		/**
		 * Returns the length of the word resulting from the stemming process.
		 */
		public int getResultLength() {
			return i_end;
		}

		/**
		 * Returns a reference to a character buffer containing the results of the
		 * stemming process. You also need to consult getResultLength() to determine the
		 * length of the result.
		 */
		public char[] getResultBuffer() {
			return b;
		}

		/* cons(i) is true <=> b[i] is a consonant. */

		private final boolean cons(int i) {
			switch (b[i]) {
			case 'a':
			case 'e':
			case 'i':
			case 'o':
			case 'u':
				return false;
			case 'y':
				return (i == 0) ? true : !cons(i - 1);
			default:
				return true;
			}
		}

		/*
		 * m() measures the number of consonant sequences between 0 and j. if c is a
		 * consonant sequence and v a vowel sequence, and <..> indicates arbitrary
		 * presence,
		 * 
		 * <c><v> gives 0 <c>vc<v> gives 1 <c>vcvc<v> gives 2 <c>vcvcvc<v> gives 3 ....
		 */

		private final int m() {
			int n = 0;
			int i = 0;
			while (true) {
				if (i > j)
					return n;
				if (!cons(i))
					break;
				i++;
			}
			i++;
			while (true) {
				while (true) {
					if (i > j)
						return n;
					if (cons(i))
						break;
					i++;
				}
				i++;
				n++;
				while (true) {
					if (i > j)
						return n;
					if (!cons(i))
						break;
					i++;
				}
				i++;
			}
		}

		/* vowelinstem() is true <=> 0,...j contains a vowel */

		private final boolean vowelinstem() {
			int i;
			for (i = 0; i <= j; i++)
				if (!cons(i))
					return true;
			return false;
		}

		/* doublec(j) is true <=> j,(j-1) contain a double consonant. */

		private final boolean doublec(int j) {
			if (j < 1)
				return false;
			if (b[j] != b[j - 1])
				return false;
			return cons(j);
		}

		/*
		 * cvc(i) is true <=> i-2,i-1,i has the form consonant - vowel - consonant and
		 * also if the second c is not w,x or y. this is used when trying to restore an
		 * e at the end of a short word. e.g.
		 * 
		 * cav(e), lov(e), hop(e), crim(e), but snow, box, tray.
		 * 
		 */

		private final boolean cvc(int i) {
			if (i < 2 || !cons(i) || cons(i - 1) || !cons(i - 2))
				return false;
			{
				int ch = b[i];
				if (ch == 'w' || ch == 'x' || ch == 'y')
					return false;
			}
			return true;
		}

		private final boolean ends(String s) {
			int l = s.length();
			int o = k - l + 1;
			if (o < 0)
				return false;
			for (int i = 0; i < l; i++)
				if (b[o + i] != s.charAt(i))
					return false;
			j = k - l;
			return true;
		}

		/*
		 * setto(s) sets (j+1),...k to the characters in the string s, readjusting k.
		 */

		private final void setto(String s) {
			int l = s.length();
			int o = j + 1;
			for (int i = 0; i < l; i++)
				b[o + i] = s.charAt(i);
			k = j + l;
		}

		/* r(s) is used further down. */

		private final void r(String s) {
			if (m() > 0)
				setto(s);
		}

		/*
		 * step1() gets rid of plurals and -ed or -ing. e.g.
		 * 
		 * caresses -> caress ponies -> poni ties -> ti caress -> caress cats -> cat
		 * 
		 * feed -> feed agreed -> agree disabled -> disable
		 * 
		 * matting -> mat mating -> mate meeting -> meet milling -> mill messing -> mess
		 * 
		 * meetings -> meet
		 * 
		 */

		private final void step1() {
			if (b[k] == 's') {
				if (ends("sses"))
					k -= 2;
				else if (ends("ies"))
					setto("i");
				else if (b[k - 1] != 's')
					k--;
			}
			if (ends("eed")) {
				if (m() > 0)
					k--;
			} else if ((ends("ed") || ends("ing")) && vowelinstem()) {
				k = j;
				if (ends("at"))
					setto("ate");
				else if (ends("bl"))
					setto("ble");
				else if (ends("iz"))
					setto("ize");
				else if (doublec(k)) {
					k--;
					{
						int ch = b[k];
						if (ch == 'l' || ch == 's' || ch == 'z')
							k++;
					}
				} else if (m() == 1 && cvc(k))
					setto("e");
			}
		}

		/* step2() turns terminal y to i when there is another vowel in the stem. */

		private final void step2() {
			if (ends("y") && vowelinstem())
				b[k] = 'i';
		}

		/*
		 * step3() maps double suffices to single ones. so -ization ( = -ize plus
		 * -ation) maps to -ize etc. note that the string before the suffix must give
		 * m() > 0.
		 */

		private final void step3() {
			if (k == 0)
				return;
			/* For Bug 1 */ switch (b[k - 1]) {
			case 'a':
				if (ends("ational")) {
					r("ate");
					break;
				}
				if (ends("tional")) {
					r("tion");
					break;
				}
				break;
			case 'c':
				if (ends("enci")) {
					r("ence");
					break;
				}
				if (ends("anci")) {
					r("ance");
					break;
				}
				break;
			case 'e':
				if (ends("izer")) {
					r("ize");
					break;
				}
				break;
			case 'l':
				if (ends("bli")) {
					r("ble");
					break;
				}
				if (ends("alli")) {
					r("al");
					break;
				}
				if (ends("entli")) {
					r("ent");
					break;
				}
				if (ends("eli")) {
					r("e");
					break;
				}
				if (ends("ousli")) {
					r("ous");
					break;
				}
				break;
			case 'o':
				if (ends("ization")) {
					r("ize");
					break;
				}
				if (ends("ation")) {
					r("ate");
					break;
				}
				if (ends("ator")) {
					r("ate");
					break;
				}
				break;
			case 's':
				if (ends("alism")) {
					r("al");
					break;
				}
				if (ends("iveness")) {
					r("ive");
					break;
				}
				if (ends("fulness")) {
					r("ful");
					break;
				}
				if (ends("ousness")) {
					r("ous");
					break;
				}
				break;
			case 't':
				if (ends("aliti")) {
					r("al");
					break;
				}
				if (ends("iviti")) {
					r("ive");
					break;
				}
				if (ends("biliti")) {
					r("ble");
					break;
				}
				break;
			case 'g':
				if (ends("logi")) {
					r("log");
					break;
				}
			}
		}

		/* step4() deals with -ic-, -full, -ness etc. similar strategy to step3. */

		private final void step4() {
			switch (b[k]) {
			case 'e':
				if (ends("icate")) {
					r("ic");
					break;
				}
				if (ends("ative")) {
					r("");
					break;
				}
				if (ends("alize")) {
					r("al");
					break;
				}
				break;
			case 'i':
				if (ends("iciti")) {
					r("ic");
					break;
				}
				break;
			case 'l':
				if (ends("ical")) {
					r("ic");
					break;
				}
				if (ends("ful")) {
					r("");
					break;
				}
				break;
			case 's':
				if (ends("ness")) {
					r("");
					break;
				}
				break;
			}
		}

		/* step5() takes off -ant, -ence etc., in context <c>vcvc<v>. */

		private final void step5() {
			if (k == 0)
				return;
			/* for Bug 1 */ switch (b[k - 1]) {
			case 'a':
				if (ends("al"))
					break;
				return;
			case 'c':
				if (ends("ance"))
					break;
				if (ends("ence"))
					break;
				return;
			case 'e':
				if (ends("er"))
					break;
				return;
			case 'i':
				if (ends("ic"))
					break;
				return;
			case 'l':
				if (ends("able"))
					break;
				if (ends("ible"))
					break;
				return;
			case 'n':
				if (ends("ant"))
					break;
				if (ends("ement"))
					break;
				if (ends("ment"))
					break;
				/* element etc. not stripped before the m */
				if (ends("ent"))
					break;
				return;
			case 'o':
				if (ends("ion") && j >= 0 && (b[j] == 's' || b[j] == 't'))
					break;
				/* j >= 0 fixes Bug 2 */
				if (ends("ou"))
					break;
				return;
			/* takes care of -ous */
			case 's':
				if (ends("ism"))
					break;
				return;
			case 't':
				if (ends("ate"))
					break;
				if (ends("iti"))
					break;
				return;
			case 'u':
				if (ends("ous"))
					break;
				return;
			case 'v':
				if (ends("ive"))
					break;
				return;
			case 'z':
				if (ends("ize"))
					break;
				return;
			default:
				return;
			}
			if (m() > 1)
				k = j;
		}

		/* step6() removes a final -e if m() > 1. */

		private final void step6() {
			j = k;
			if (b[k] == 'e') {
				int a = m();
				if (a > 1 || a == 1 && !cvc(k - 1))
					k--;
			}
			if (b[k] == 'l' && doublec(k) && m() > 1)
				k--;
		}

		/**
		 * Stem the word placed into the Stemmer buffer through calls to add(). Returns
		 * true if the stemming process resulted in a word different from the input. You
		 * can retrieve the result with getResultLength()/getResultBuffer() or
		 * toString().
		 */
		public void stem() {
			k = i - 1;
			if (k > 1) {
				step1();
				step2();
				step3();
				step4();
				step5();
				step6();
			}
			i_end = k + 1;
			i = 0;
		}

		/**
		 * Test program for demonstrating the Stemmer. It reads text from a a list of
		 * files, stems each word, and writes the result to standard output. Note that
		 * the word stemmed is expected to be in lower case: forcing lower case must be
		 * done outside the Stemmer class. Usage: Stemmer file-name file-name ...
		 */
	}
}