package backend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import kvs.KVSClient;
import kvs.Row;
import tools.Hasher;

public class Ranker {
	public static List<String> rank(KVSClient kvs, String[] searchTerms) throws IOException {
		// compute cosine similarity for each document that contains at least one search term		
		// step 1 - map each URL that contains at least one search term to its word count
		Map<String, Integer> urlToWordCount = new HashMap<>();
		Map<String, Map<String, String[]>> outerMap = new HashMap<>();
		for (String term : searchTerms) {
			Map<String, String[]> innerMap = new HashMap<>();
			Row row = kvs.getRow("index", term);
			if (row != null) {
//				System.out.println("value for row " + row.key() + " is: " + row.get("value"));
				String[] urlsAndFreqs = row.get("value").split(",");
				for (String s : urlsAndFreqs) {
					int pos = s.lastIndexOf(":");
					if (pos > 0) {
						String url = s.substring(0, pos);
						String[] positions = s.substring(pos+1).split(" ");
						innerMap.put(url, positions);
						if (!urlToWordCount.containsKey(url)) {
							String page = new String(kvs.get("crawl", Hasher.hash(url), "page"));
							page = page.replaceAll("[.,:;!\\?\'\"()-]", " ").replaceAll("<[^>]*>", "");
			                int wordCount = page.split("\\s+").length;
							urlToWordCount.put(url, wordCount);
						}
					}
				}
			}
			outerMap.put(term, innerMap);
		}

		// N is the total number of URLs that contain at least one search term
		int N = urlToWordCount.size();
		
		// step 2 - map each URL that contains at least one search term to an array of integers, which
		// represents the frequencies of each search term in that URL
		Map<String, int[]> urlToFrequencies = new HashMap<>();
		Double[] idfArray = new Double[searchTerms.length];
		int cur = 0;
		
		for (int i = 0; i < searchTerms.length; i++, cur++) {
			String term = searchTerms[i];
			Row row = kvs.getRow("index", term);
			if (row != null) {
				String[] urlsAndFreqs = row.get("value").split(",");
				int n = urlsAndFreqs.length;
				double idf = N / n;
				idfArray[cur] = idf;
				
				Map<String, Integer> map = new HashMap<>();
				for (String s : urlsAndFreqs) {
					int pos = s.lastIndexOf(":");
					if (pos > 0) {
						String url = s.substring(0, pos);
	        			int freq = s.substring(pos+1).split(" ").length;
	        			map.put(url, freq);
					}
				}
				
				for (String url : urlToWordCount.keySet()) {
    				if (map.containsKey(url)) {
    					int freq = map.get(url);
    					if (urlToFrequencies.containsKey(url)) {
	        				urlToFrequencies.get(url)[cur] = freq;
	        			} else {
	        				int[] freqs = new int[searchTerms.length];
	        				freqs[cur] = freq;
	        				urlToFrequencies.put(url, freqs);
	        			}
    				} else {
    					if (urlToFrequencies.containsKey(url)) {
	        				urlToFrequencies.get(url)[cur] = 0;
	        			} else {
	        				int[] freqs = new int[searchTerms.length];
	        				freqs[cur] = 0;
	        				urlToFrequencies.put(url, freqs);
	        			}
    				}
    			}
			} else {
				idfArray[cur] = 0.0;
				for (String url : urlToFrequencies.keySet()) {
					urlToFrequencies.get(url)[cur] = 0;
				}
			}
		}
		
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
		
		// step 4 - perform phrase search and find URLs that contain the exact match of the search terms,
		// and also URLs that contain a near exact match (defined as terms which has one less word than
		// the original search terms, but otherwise maintain the same word order, e.g. the original search
		// term is "hello world cup", and its near exact matches are "hello world", "hello cup" and "world cup")
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
		
//		System.out.println("urlsWithExactMatch: " + urlsWithExactMatch);
//		System.out.println("urlsWithNearExactMatch: " + urlsWithNearExactMatch);
		
		// step 5 - compute TF-IDF cosine scores for each URL that contains at least one search term,
		// and compute the final scores by combining cosine scores with page ranks
		Map<String, Double> finalScores = new TreeMap<>(Collections.reverseOrder());
		for (Map.Entry<String, int[]> entry : urlToFrequencies.entrySet()) {
			String url = entry.getKey();
			int[] freqs = entry.getValue();
//			System.out.println("freqs for url " + url + " is: " + Arrays.toString(freqs));
			double cosineScore = 0.0;
			for (int i = 0; i < searchTerms.length; i++) {
				cosineScore += freqs[i] * idfArray[i];
			}
			int wordCount = urlToWordCount.get(url);
			cosineScore /= wordCount;
//			System.out.println("cosine score for url " + url + " is: " + cosineScore);
			
			// compute the final scores by multiplying cosine scores and page ranks
			Row row = kvs.getRow("pageranks", Hasher.hash(url));
			if (row != null) {
				for (String col: row.columns()) {
					String val = row.get(col);
					if (val != null) {
						String[] urlAndPagerank = val.split(",");
						if (urlAndPagerank.length == 2) {
							double pageRank = Double.valueOf(urlAndPagerank[1]);
//							System.out.println("page rank for url " + url + " is: " + pageRank);
							double finalScore = cosineScore * pageRank * 1000;
							if (finalScore > 10) System.out.println("initial score for url " + url + " is: " + finalScore);
							if (urlsWithExactMatch.contains(url)) {
								finalScore += 500;
//								System.out.println("bumped score for url " + url + " is: " + finalScore);
							} else if (urlsWithNearExactMatch.contains(url)) {
								finalScore += 300;
//								System.out.println("bumped score for url " + url + " is: " + finalScore);
							}
//							System.out.println("final score for url " + url + " is: " + finalScore);
							finalScores.put(url, finalScore);
							break;
						}
					}
				}
			}
		}		
		
		// step 6 - rank URLs first by searchTermCount, then by finalScore, both in descending order
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
//		Iterator<Entry<Double, String>> iterator = finalScores.entrySet().iterator();
		for (int i = 0; i < K; i++) {
			if (i < list.size()) {
				outputURLs.add(list.get(i).getURL());
			}
//			if (iterator.hasNext()) {
//				outputURLs.add(iterator.next().getValue());
//	        }
		}
		return outputURLs;
	}
	
	private static boolean urlContainsExactTerms(String url, String[] searchTerms, Map<String, Map<String, String[]>> outerMap) {
		List<String[]> termPositions = new ArrayList<>();
		for (String term : searchTerms) {
			String[] positions = outerMap.get(term).get(url);
			termPositions.add(positions);
//			System.out.println("positions for term " + term + " is: " + Arrays.toString(positions));
		}
		
		int[] array = new int[searchTerms.length];
		for (int i = 0; i < array.length - 1; i++) {
			String[] positions1 = termPositions.get(i);
			String[] positions2 = termPositions.get(i+1);
			while (array[i] < positions1.length && array[i+1] < positions2.length) {
				if (Integer.valueOf(positions1[array[i]]) + 1 == Integer.valueOf(positions2[array[i+1]])) {
					// the URL contains the current pair of terms contiguously, so we keep checking the next pair
					break;
				} else if (Integer.valueOf(positions1[array[i]]) + 1 < Integer.valueOf(positions2[array[i+1]])) {
					array[i]++;
				} else {
					array[i+1]++;
				}
			}
//			System.out.println("array for url " + url + " is: " + Arrays.toString(array));
			if (array[i] >= positions1.length || array[i+1] >= positions2.length) {
				// the URL does not contain the exact search terms
				return false;
			}
		}
		
		return true;
	}
}
