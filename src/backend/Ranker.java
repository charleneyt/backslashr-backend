package backend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import kvs.KVSClient;
import kvs.Row;
import tools.Hasher;

public class Ranker {
	public static List<String> rank(KVSClient kvs, String[] searchTerms) throws IOException {
		// compute cosine similarity for each document that contains at least one search term
		Map<String, Integer> urlToWordCount = new HashMap<>();
		for (String term : searchTerms) {
			Row row = kvs.getRow("index", term);
			if (row != null) {
				String[] urlsAndFreqs = row.get("value").split(",");
				for (String s : urlsAndFreqs) {
					int pos = s.lastIndexOf(":");
					if (pos > 0) {
						String url = s.substring(0, pos);
						if (!urlToWordCount.containsKey(url)) {
							String page = new String(kvs.get("crawl", Hasher.hash(url), "page"));
							page = page.replaceAll("[.,:;!\\?\'\"()-]", " ").replaceAll("<[^>]*>", "");
			                int wordCount = page.split("\\s+").length;
							urlToWordCount.put(url, wordCount);
						}
					}
				}
			}
		}
		
		int N = urlToWordCount.size();
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

//		System.out.println("idfArray is: " + Arrays.toString(idfArray));
//		for (String url : urlToFrequencies.keySet()) {
//			System.out.println("~~~freqs for url " + url + " is: " + Arrays.toString(urlToFrequencies.get(url)));
//		}
		
		Map<Double, String> finalScores = new TreeMap<>(Collections.reverseOrder());
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
			Row row = kvs.getRow("pageranks", url);
			if (row != null) {
				double pageRank = Double.valueOf(row.get(url + "0"));
//				double pageRank = Double.valueOf(row.get("rank"));
//				System.out.println("page rank for url " + url + " is: " + pageRank);
				double finalScore = cosineScore * pageRank * 1000;
//				System.out.println("final score for url " + url + " is: " + finalScore);
				finalScores.put(finalScore, url);
			}
		}
		
		int K = 100; // number of search results to display on front end
		List<String> outputURLs = new ArrayList<>();
		Iterator<Entry<Double, String>> iterator = finalScores.entrySet().iterator();
		for (int i = 0; i < K; i++) {
			if (iterator.hasNext()) {
				outputURLs.add(iterator.next().getValue());
	        }
		}
		return outputURLs;
	}
}
