package backend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import jobs.Crawler;
import kvs.KVSClient;
import kvs.Row;
import tools.Hasher;

public class Ranker {
	public static Map<String, Integer> urlToPreviewIndex = new HashMap<>();
	public static final int MAX_THREAD_COUNT = 20;
	public static final int MAX_URL = 500;// 19 22 21 340

	public static List<String> rank(KVSClient kvs, String[] searchTerms) {
		// step 1 - map each URL that contains at least one search term to its word
		// count; also, make an outerMap whose keys are words in the search terms,
		// and the corresponding value of each key is an innerMap, whose keys are
		// URLs that contain the word, and the corresponding value of each URL is
		// an array of indices at which the word appears in that URL
		Map<String, Integer> urlToWordCount = new HashMap<>();
		Map<String, Map<String, String[]>> outerMap = new HashMap<>();
		for (String term : searchTerms) {
//			System.out.println("Term is: " + term);
			Map<String, String[]> innerMap = new HashMap<>();
			Row row = null;
			try {
				row = kvs.getRow("index_final", term);
			} catch (IOException e1) {
				System.out.println("Failed to get row for " + term);
			}
//			System.out.println("row is: " + row.key());
			if (row == null) {
				continue;
			}

			String[] urlsAndFreqs = row.get("value").split(",");
			int thread_count = Math.min(MAX_THREAD_COUNT, urlsAndFreqs.length);
			int url_per_thread = Math.min(MAX_URL, urlsAndFreqs.length) / thread_count;
			int extra_url_thread_index = Math.min(MAX_URL, urlsAndFreqs.length) % thread_count;
//			System.out.println("url_per_thread " + url_per_thread + " extra_url_thread_index " + extra_url_thread_index);
			Thread threads[] = new Thread[thread_count];
			for (int i = 0; i < thread_count; i++) {
				final int k = i;
				final int extar_url_count = i < extra_url_thread_index ? 1 : 0;
				final int total_extar_url_count = i < extra_url_thread_index ? i : extra_url_thread_index;
				threads[i] = new Thread() {
					public void run() {
						for (int j = total_extar_url_count + k * url_per_thread; j < extar_url_count + Math
								.min(total_extar_url_count + (k + 1) * url_per_thread, urlsAndFreqs.length); j++) {
							try {
								String s = urlsAndFreqs[j];
								int pos = s.lastIndexOf(":");
								if (pos <= 0)
									continue;

								String url = s.substring(0, pos);
								String[] positions = s.substring(pos + 1).split(" ");
								innerMap.put(url, positions);

								if (positions.length > 0) {
									String previewIndex = positions[0];
									if (!urlToPreviewIndex.containsKey(url)) {
										try {
											urlToPreviewIndex.put(url, Integer.valueOf(previewIndex));
										} catch (Exception e) {

										}
									}
								}

								if (urlToWordCount.containsKey(url))
									continue;

								byte[] wordCount;
								if ((wordCount = kvs.get("content", Hasher.hash(url), "wordCount")) != null) {
									urlToWordCount.put(url, Integer.valueOf(new String(wordCount)));
								} else {
									urlToWordCount.put(url, 1000);
								}
							} catch (NumberFormatException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				};

				threads[i].start();
			}
			for (int i = 0; i < threads.length; i++) {
				try {
					threads[i].join();
				} catch (InterruptedException ie) {
					System.out.println("Failed to join thread " + i);
				}
			}

			outerMap.put(term, innerMap);
		}

		// N is the total number of URLs that contain at least one search term
		int N = urlToWordCount.size();
//		System.out.println("N is: " + N);

		// step 2 - map each URL that contains at least one search term to an array of
		// integers, which represents the frequencies of each search term in that URL
		Map<String, int[]> urlToFrequencies = new HashMap<>();
		Double[] idfArray = new Double[searchTerms.length];
		int cur = 0;
		for (int i = 0; i < searchTerms.length; i++, cur++) {
			String term = searchTerms[i];
			if (outerMap.containsKey(term)) {
				int n = outerMap.get(term).size();
				if (n > 0) {
					double idf = N / n;
					idfArray[cur] = idf;

					Map<String, Integer> map = new HashMap<>();
					for (Map.Entry<String, String[]> entry : outerMap.get(term).entrySet()) {
						String url = entry.getKey();
						int freq = entry.getValue().length;
						map.put(url, freq);
					}
					for (String url : urlToWordCount.keySet()) {
						int freq = map.containsKey(url) ? map.get(url) : 0;
						if (urlToFrequencies.containsKey(url)) {
							urlToFrequencies.get(url)[cur] = freq;
						} else {
							int[] freqs = new int[searchTerms.length];
							freqs[cur] = freq;
							urlToFrequencies.put(url, freqs);
						}
					}
				} else {
					idfArray[cur] = 0.0;
					for (String url : urlToFrequencies.keySet()) {
						urlToFrequencies.get(url)[cur] = 0;
					}
				}
			}
		}
//		System).out.println("urlToFrequencies = " + urlToFrequencies);
		// step 3 - map each URL that contains at least one search term to the number of
		// unique search terms in that URL
		Map<String, Integer> urlToSearchTermCounts = new TreeMap<>(Collections.reverseOrder());
		for (Map.Entry<String, int[]> entry : urlToFrequencies.entrySet()) {
			String url = entry.getKey();
			int[] freqs = entry.getValue();
			int count = 0;
			for (int freq : freqs) {
				if (freq != 0) {
					count++;
				}
			}
			urlToSearchTermCounts.put(url, count);
		}
//		System.out.println("urlToSearchTermCounts = " + urlToSearchTermCounts);

		// step 4 - perform phrase search and find URLs that contain the exact match of
		// the search terms, and also URLs that contain a near exact match (defined as
		// terms which have one less word than the original search terms, but otherwise
		// maintain the same word order, e.g. the original search term is
		// "hello world cup", and its near exact matches are "hello world",
		// "hello cup" and "world cup")
		List<String> urlsWithExactMatch = new ArrayList<>();
		List<String> urlsWithNearExactMatch = new ArrayList<>();
		for (Map.Entry<String, Integer> entry : urlToSearchTermCounts.entrySet()) {
			String url = entry.getKey();
			int searchTermCount = entry.getValue();
			// the current URL is a candidate that might contain the exact search terms
			if (searchTermCount == searchTerms.length) {
				boolean currentUrlContainsExactSearchTerms = urlContainsExactTerms(url, searchTerms, outerMap);
				if (!currentUrlContainsExactSearchTerms) { // continue to check the next URL
					continue;
				} else {
					urlsWithExactMatch.add(url);
				}
			} else if (searchTermCount == searchTerms.length - 1) {
				StringBuilder nearExactMatch = new StringBuilder();
				int[] freqs = urlToFrequencies.get(url);
				for (int i = 0; i < freqs.length; i++) {
					if (freqs[i] != 0) {
						nearExactMatch.append(searchTerms[i] + " ");
					}
				}

				if (urlContainsExactTerms(url, nearExactMatch.toString().split(" "), outerMap)) {
//					System.out.println("nearExactMatch for url "+url+" is: " + nearExactMatch);
					urlsWithNearExactMatch.add(url);
				}
			}
		}
//
//		System.out.println("urlsWithExactMatch: " + urlsWithExactMatch);
//		System.out.println("urlsWithNearExactMatch: " + urlsWithNearExactMatch);

		// step 5 - compute TF-IDF cosine scores for each URL that contains at least
		// one search term, and compute the final scores by combining cosine scores
		// with page ranks

		ArrayList<String> urlList = new ArrayList<String>(urlToFrequencies.keySet());

//		int thread_count = Math.min(MAX_THREAD_COUNT, urlList.size());
		int thread_count = 1;
		if (MAX_THREAD_COUNT < urlList.size()) {
			thread_count = MAX_THREAD_COUNT;
		} else if (urlList.size() > 0) {
			thread_count = urlList.size();
		}
		int url_per_thread = Math.min(MAX_URL, urlList.size()) / thread_count;
		int extra_url_thread_index = Math.min(MAX_URL, urlList.size()) % thread_count;

		Map<String, Double> finalScores = new TreeMap<>(Collections.reverseOrder());
		Thread threads[] = new Thread[thread_count];
//	    for (Map.Entry<String, int[]> entry : urlToFrequencies.entrySet()) {
		for (int i = 0; i < thread_count; i++) {
			final int k = i;
			final int extar_url_count = i < extra_url_thread_index ? 1 : 0;
			final int total_extar_url_count = i < extra_url_thread_index ? i : extra_url_thread_index;
			threads[i] = new Thread() {
				public void run() {
					for (int j = total_extar_url_count + k * url_per_thread; j < extar_url_count
							+ Math.min(total_extar_url_count + (k + 1) * url_per_thread, urlList.size()); j++) {
						String url = urlList.get(j);
						int[] freqs = urlToFrequencies.get(url);
//		    			System.out.println("freqs for url " + url );
						double cosineScore = 0.0;
						for (int i = 0; i < searchTerms.length; i++) {
							cosineScore += freqs[i] * idfArray[i];
						}
						int wordCount = urlToWordCount.get(url);
						cosineScore /= wordCount;
						// System.out.println("cosine score for url " + url + " is: " + cosineScore);

						// compute the final scores by multiplying cosine scores and page ranks
						double finalScore = cosineScore * 1000;
						Row row;
						try {
							row = kvs.getRow("pageranks", Hasher.hash(url));
							if (row != null && row.get(Hasher.hash(url) + "0") != null) {
								// System.out.println("page rank for url " + Hasher.hash(url) + " is: " +
								// row.get(Hasher.hash(url) + "0"));
								double pageRank = Double.valueOf(row.get(Hasher.hash(url) + "0"));
								// System.out.println("page rank for url " + url + " is: " + pageRank);
								finalScore += pageRank;
							} else {
								for (String hub : Crawler.authorityHubs) {
									if (url.contains(hub)) {
										finalScore += 1;
									}
								}
							}

							if (urlsWithExactMatch.contains(url)) {
								finalScore += 1000;
								// System.out.println("bumped score for url " + url + " is: " + finalScore);
							} else if (urlsWithNearExactMatch.contains(url)) {
								finalScore += 600;
								// System.out.println("bumped score for url " + url + " is: " + finalScore);
							}
							// System.out.println("final score for url " + url + " is: " + finalScore);
							finalScores.put(url, finalScore);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			};

			threads[i].start();
		}

		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ie) {
				System.out.println("Failed to join thread " + i);
			}
		}

		// step 6 - rank URLs first by searchTermCount, then by finalScore, both in
		// descending order
		List<URLWithScores> list = new ArrayList<>();
		for (String url : urlToFrequencies.keySet()) {
			Integer searchTermCount = urlToSearchTermCounts.get(url);
			Double finalScore = finalScores.get(url);
			if (searchTermCount != null && finalScore != null) {
				URLWithScores urlWithScores = new URLWithScores(url, searchTermCount, finalScore);
				list.add(urlWithScores);
			}
		}

		Collections.sort(list);

		// step 7 - choose K highest ranking URLs to display on front end
		int K = 100;
		List<String> outputURLs = new ArrayList<>();
		for (int j = 0; j < K; j++) {
			if (j < list.size()) {
				outputURLs.add(list.get(j).getURL());
			}
		}
		return outputURLs;
	}

	private static boolean urlContainsExactTerms(String url, String[] searchTerms,
			Map<String, Map<String, String[]>> outerMap) {
		List<String[]> termPositions = new ArrayList<>();
		for (String term : searchTerms) {
			// Wangxing to double check
			if (outerMap.containsKey(term) && outerMap.get(term).containsKey(url)) {
				String[] positions = outerMap.get(term).get(url);
				termPositions.add(positions);
				// System.out.println("positions for term " + term + " is: " +
				// Arrays.toString(positions));
			}
		}

		int[] array = new int[searchTerms.length];
		for (int i = 0; i < array.length - 1; i++) {
			String[] positions1 = termPositions.get(i);
			String[] positions2 = termPositions.get(i + 1);
			while (array[i] < positions1.length && array[i + 1] < positions2.length) {
				try {
					if (Integer.valueOf(positions1[array[i]]) + 1 == Integer.valueOf(positions2[array[i + 1]])) {
						// the URL contains the current pair of terms contiguously, so we keep checking
						// the next pair
						break;
					} else if (Integer.valueOf(positions1[array[i]]) + 1 < Integer.valueOf(positions2[array[i + 1]])) {
						array[i]++;
					} else {
						array[i + 1]++;
					}
				} catch (Exception e) {
//					System.out.println("Integer.valueof Error");
					return false;
				}
			}
//			System.out.println("array for url " + url + " is: " + Arrays.toString(array));
			if (array[i] >= positions1.length || array[i + 1] >= positions2.length) {
				// the URL does not contain the exact search terms
				return false;
			}
		}

		return true;
	}
}
