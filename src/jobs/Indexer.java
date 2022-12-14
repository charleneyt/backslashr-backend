package jobs;

import flame.*;
import kvs.*;
import tools.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class Indexer {
	static boolean debugMode = false;
	public static void run(FlameContext ctx, String[] args) throws Exception {
		System.out.println("Executing indexer ...updated as of 12/10 at " + new Date());

		// read through content table, filter out nonvalid words and fill out the index
		// table
		long startGetTime = System.currentTimeMillis();
		System.out.println("Started processing from content table at " + new Date());

		FlameRDD transform = ctx.indexFromTable("content", (r, dict) -> {
			String url = r.get("url");
			String page = r.get("page");
			FileWriter fw;
			if (debugMode) {
				try {
					fw = new FileWriter("indexer_log", true);
					fw.write("url operated: " + url + "\n");
					fw.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}				
			}

			if (page != null && page.length() > 0) {
				String content = page.replaceAll("[.,:;!\\?\'\"()-]", " ").toLowerCase();
				String[] words = content.split("\\s+");
				if (FlameContext.getKVS() == null) {
					FlameContext.setKVS("localhost:8000");
				}
				KVSClient kvs = FlameContext.getKVS();
				try {
					kvs.put("content", Hasher.hash(url), "wordCount", String.valueOf(words.length));
				} catch (IOException e1) {
					e1.printStackTrace();
					if (debugMode) {
						try {
							fw = new FileWriter("indexer_log", true);
							fw.write("failed to put word count for " + url + " to content.\n");
							fw.close();
						} catch (IOException e) {
							e.printStackTrace();
						}				
					}
				}				

				HashMap<String, ArrayList<Integer>> wordToPosByUrl = new HashMap<>();
				for (int i = 0; i < words.length; i++) {
					// check whether the word is a dictionary word
					boolean isWord = dict.contains(words[i]);
					if (!isWord)
						continue;						

					if (!wordToPosByUrl.containsKey(words[i]))
						wordToPosByUrl.put(words[i], new ArrayList<Integer>());
					
					// if the same word has appeared in the same page for 50 times, ignore additional positions
					if (wordToPosByUrl.get(words[i]).size() >= 50)
						continue;
					 wordToPosByUrl.get(words[i]).add(i + 1);
				}
				
				// put together word : row hashmap to be sent as a table
				HashMap<String, Row> wordToCombined = new HashMap<>();
				
				for (String word : wordToPosByUrl.keySet()) {
					StringBuilder sb = new StringBuilder();
					for (Integer pos : wordToPosByUrl.get(word)) {
						if (sb.length() == 0)
							sb.append(pos + "");
						else {
							sb.append(" " + pos);							
						}
					}
					String combined = url + ":" + sb.toString();
					Row row = new Row(word);
					row.put("value", combined);
					wordToCombined.put(word, row);
				}	
				try {
					kvs.putTable("index_imm", wordToCombined);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					if (debugMode) {
						try {
							fw = new FileWriter("indexer_log", true);
							fw.write("failed to put rows for " + url + " to index_imm.\n");
							fw.close();
						} catch (IOException e1) {
							e1.printStackTrace();
						}				
					}
				}
			}

			return url;
		});
		long endGetTime = System.currentTimeMillis();
		System.out.println("Finished indexing step 1 from content table! Took " + (endGetTime - startGetTime) + " ms.");
	}
}