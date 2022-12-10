package kvs;

import java.util.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.io.*;
import tools.HTTP;

public class KVSClient implements KVS {

	String master;

	static class WorkerEntry implements Comparable<WorkerEntry> {
		String address;
		String id;

		WorkerEntry(String addressArg, String idArg) {
			address = addressArg;
			id = idArg;
		}

		public int compareTo(WorkerEntry e) {
			return id.compareTo(e.id);
		}
	};

	Vector<WorkerEntry> workers;
	boolean haveWorkers;

	public int numWorkers() throws IOException {
		if (!haveWorkers)
			downloadWorkers();
		return workers.size();
	}

	public static String getVersion() {
		return "v1.3 Oct 28 2022";
	}

	public String getMaster() {
		return master;
	}

	public String getWorkerAddress(int idx) throws IOException {
		if (!haveWorkers)
			downloadWorkers();
		return workers.elementAt(idx).address;
	}

	public String getWorkerID(int idx) throws IOException {
		if (!haveWorkers)
			downloadWorkers();
		return workers.elementAt(idx).id;
	}

	class KVSIterator implements Iterator<Row> {
		InputStream in;
		boolean atEnd;
		Row nextRow;
		int currentRangeIndex;
		String endRowExclusive;
		String startRow;
		String tableName;
		Vector<String> ranges;

		KVSIterator(String tableNameArg, String startRowArg, String endRowExclusiveArg) throws IOException {
			in = null;
			currentRangeIndex = 0;
			atEnd = false;
			endRowExclusive = endRowExclusiveArg;
			tableName = tableNameArg;
			startRow = startRowArg;
			ranges = new Vector<String>();
			if ((startRowArg == null) || (startRowArg.compareTo(getWorkerID(0)) < 0)) {
				String url = getURL(tableNameArg, numWorkers() - 1, startRowArg,
						((endRowExclusiveArg != null) && (endRowExclusiveArg.compareTo(getWorkerID(0)) < 0))
								? endRowExclusiveArg
								: getWorkerID(0));
				ranges.add(url);
			}
			for (int i = 0; i < numWorkers(); i++) {
				if ((startRowArg == null) || (i == numWorkers() - 1)
						|| (startRowArg.compareTo(getWorkerID(i + 1)) < 0)) {
					if ((endRowExclusiveArg == null) || (endRowExclusiveArg.compareTo(getWorkerID(i)) > 0)) {
						boolean useActualStartRow = (startRowArg != null)
								&& (startRowArg.compareTo(getWorkerID(i)) > 0);
						boolean useActualEndRow = (endRowExclusiveArg != null) && ((i == (numWorkers() - 1))
								|| (endRowExclusiveArg.compareTo(getWorkerID(i + 1)) < 0));
						String url = getURL(tableNameArg, i, useActualStartRow ? startRowArg : getWorkerID(i),
								useActualEndRow ? endRowExclusiveArg
										: ((i < numWorkers() - 1) ? getWorkerID(i + 1) : null));
						ranges.add(url);
					}
				}
			}

			openConnectionAndFill();
		}

		protected String getURL(String tableNameArg, int workerIndexArg, String startRowArg, String endRowExclusiveArg)
				throws IOException {
			String params = "";
			if (startRowArg != null)
				params = "startRow=" + startRowArg;
			if (endRowExclusiveArg != null)
				params = (params.equals("") ? "" : (params + "&")) + "endRowExclusive=" + endRowExclusiveArg;
			return "http://" + getWorkerAddress(workerIndexArg) + "/data/" + tableNameArg
					+ (params.equals("") ? "" : "?" + params);
		}

		void openConnectionAndFill() {
			try {
				if (in != null) {
					in.close();
					in = null;
				}

				if (atEnd)
					return;

				while (true) {
					if (currentRangeIndex >= ranges.size()) {
						atEnd = true;
						return;
					}

					try {
						URL url = new URL(ranges.elementAt(currentRangeIndex));
						HttpURLConnection con = (HttpURLConnection) url.openConnection();
						con.setRequestMethod("GET");
						// Update where to put timeout Cindy 12/02
						con.connect();
						// con.setConnectTimeout(5000);
						// con.setReadTimeout(10000);
						in = con.getInputStream();
						Row r = fill();
						if (r != null) {
							nextRow = r;
							break;
						}
					} catch (FileNotFoundException fnfe) {
					}

					currentRangeIndex++;
				}
			} catch (IOException ioe) {
				if (in != null) {
					try {
						in.close();
					} catch (Exception e) {
					}
					in = null;
				}
				atEnd = true;
			}
		}

		synchronized Row fill() {
			try {
				Row r = Row.readFrom(in);
				return r;
			} catch (Exception e) {
				return null;
			}
		}

		public synchronized Row next() {
			if (atEnd)
				return null;
			Row r = nextRow;
			nextRow = fill();
			while ((nextRow == null) && !atEnd) {
				currentRangeIndex++;
				openConnectionAndFill();
			}

			return r;
		}

		public synchronized boolean hasNext() {
			return !atEnd;
		}
	}

	synchronized void downloadWorkers() throws IOException {
		String result = new String(HTTP.doRequest("GET", "http://" + master + "/workers", null).body());
		String[] pieces = result.split("\n");
		int numWorkers = Integer.parseInt(pieces[0]);
		if (numWorkers < 1)
			throw new IOException("No active KVS workers");
		if (pieces.length != (numWorkers + 1))
			throw new RuntimeException("Received truncated response when asking KVS master for list of workers");
		workers.clear();
		for (int i = 0; i < numWorkers; i++) {
			String[] pcs = pieces[1 + i].split(",");
			workers.add(new WorkerEntry(pcs[1], pcs[0]));
		}
		Collections.sort(workers);

		haveWorkers = true;
	}

	int workerIndexForKey(String key) {
		int chosenWorker = workers.size() - 1;
		if (key != null) {
			for (int i = 0; i < workers.size() - 1; i++) {
				if ((key.compareTo(workers.elementAt(i).id) >= 0) && (key.compareTo(workers.elementAt(i + 1).id) < 0))
					chosenWorker = i;
			}
		}

		return chosenWorker;
	}

	public KVSClient(String masterArg) {
		master = masterArg;
		workers = new Vector<WorkerEntry>();
		haveWorkers = false;
	}

	public void rename(String oldTableName, String newTableName) throws IOException {
		if (!haveWorkers)
			downloadWorkers();

		for (WorkerEntry w : workers) {
			try {
				byte[] response = HTTP.doRequest("PUT",
						"http://" + w.address + "/rename/" + java.net.URLEncoder.encode(oldTableName, "UTF-8") + "/",
						newTableName.getBytes()).body();
				String result = new String(response);
			} catch (Exception e) {
			}
		}
	}

	public void delete(String tableName) throws IOException {
		if (!haveWorkers)
			downloadWorkers();

		for (WorkerEntry w : workers) {
			try {
				HTTP.doRequest("PUT",
						"http://" + w.address + "/delete/" + java.net.URLEncoder.encode(tableName, "UTF-8"), null);
			} catch (Exception e) {
			}
		}
	}
	
	public void clean(String tableName) throws IOException {
		if (!haveWorkers)
			downloadWorkers();

		for (WorkerEntry w : workers) {
			try {
				HTTP.doRequest("PUT",
						"http://" + w.address + "/clean/" + java.net.URLEncoder.encode(tableName, "UTF-8"), null);
			} catch (Exception e) {
			}
		}		
	}

	public void put(String tableName, String row, String column, byte value[]) throws IOException {
		if (!haveWorkers)
			downloadWorkers();

		try {
			String target = "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/"
					+ java.net.URLEncoder.encode(row, "UTF-8") + "/" + java.net.URLEncoder.encode(column, "UTF-8");
			// System.out.println(target);
			byte[] response = HTTP.doRequest("PUT", target, value).body();
			String result = new String(response);
			if (!result.equals("OK")) {
				FileWriter fw = new FileWriter("./KVSClient_log", true);
				fw.write("PUT returned something other than OK: " + result + "(" + target + ")");
				fw.close();
//				throw new RuntimeException("PUT returned something other than OK: " + result + "(" + target + ")");	
			}
		} catch (UnsupportedEncodingException uee) {
			throw new RuntimeException("UTF-8 encoding not supported?!?");
		}
	}

	public void put(String tableName, String row, String column, String value) throws IOException {
		put(tableName, row, column, value.getBytes());
	}

	public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
		if (!haveWorkers)
			downloadWorkers();

		byte[] response = HTTP.doRequest("PUT",
				"http://" + workers.elementAt(workerIndexForKey(row.key())).address + "/data/" + tableName,
				row.toByteArray()).body();
		String result = new String(response);
		if (!result.equals("OK"))
			throw new RuntimeException("PUT returned something other than OK: " + result);
	}
	
	private static byte[] combineTwoByteArrays(byte[] a, byte[] b) {
        byte[] result = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;	
	}
	
	// added client interface to stream PUT a table
	public void putTable(String tableName, HashMap<String, Row> wordToRow) throws FileNotFoundException, IOException {
		if (!haveWorkers)
			downloadWorkers();
		
//		System.out.println(wordToRow.toString());
		
		ArrayList<String>[] workerToKeys = new ArrayList[workers.size()];
		
		for (String rowName : wordToRow.keySet()) {
			int workerIdx = workerIndexForKey(rowName);
			if (workerToKeys[workerIdx] == null)
				workerToKeys[workerIdx] = new ArrayList<String>();
			workerToKeys[workerIdx].add(rowName);
		}
		
		for (int i = 0; i < workers.size(); i++) {
			if (workerToKeys[i] == null)
				continue;
			
			byte[] body = new byte[0];
			for (String rowName : workerToKeys[i]) {
				body = combineTwoByteArrays(body, wordToRow.get(rowName).toByteArray());
				body = combineTwoByteArrays(body, new byte[] { (byte) 0x0a });
				System.out.println("body length is: " + body.length);
				if (body.length > 10000) {
					  FileWriter fw = new FileWriter("over_500000_log", true);
					  fw.write("from: " + ProcessHandle.current().pid() + "| " + Thread.currentThread().getName() + "\n");
					  fw.write("worker " + i + ":" + new String(body, StandardCharsets.UTF_8) + "\n");
					  fw.close();
					byte[] response = HTTP.doRequest("PUT",
							"http://" + workers.elementAt(i).address + "/data/" + tableName,
							body).body();	
					String result = new String(response);
					if (!result.equals("OK"))
						throw new RuntimeException("PUT returned something other than OK: " + result);
					body = new byte[0];
				}
			}
//			body = combineTwoByteArrays(body, new byte[] { (byte) 0x0a });
			
			if (body == null || body.length == 0) {
				System.out.println("nothing in string builder!");
				continue;
			}
			
			  FileWriter fw = new FileWriter("send_put_table_log", true);
			  fw.write("from: " + ProcessHandle.current().pid() + "| " + Thread.currentThread().getName() + "\n");
			  fw.write("worker " + i + ":" + new String(body, StandardCharsets.UTF_8) + "\n");
			  fw.close();
			System.out.println("worker " + i + ":" + new String(body, StandardCharsets.UTF_8));
			
			byte[] response = HTTP.doRequest("PUT",
					"http://" + workers.elementAt(i).address + "/data/" + tableName,
					body).body();	
			String result = new String(response);
			if (!result.equals("OK"))
				throw new RuntimeException("PUT returned something other than OK: " + result);
		}

	}

	public Row getRow(String tableName, String row) throws IOException {
		System.out.println("before download workers");
		if (!haveWorkers)
			downloadWorkers();
		System.out.println("after download workers");
		System.out.println("request is: " + "http://" + workers.elementAt(workerIndexForKey(row)).address
				+ "/data/" + tableName + "/" + java.net.URLEncoder.encode(row, "UTF-8"));
		HTTP.Response resp = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address
				+ "/data/" + tableName + "/" + java.net.URLEncoder.encode(row, "UTF-8"), null);
		System.out.println("after get request");
		if (resp.statusCode() == 404)
			return null;

		byte[] result = resp.body();
		try {
			return Row.readFrom(new ByteArrayInputStream(result));
		} catch (Exception e) {
			throw new RuntimeException("Decoding error while reading Row from getRow() URL");
		}
	}

	public byte[] get(String tableName, String row, String column) throws IOException {
		if (!haveWorkers)
			downloadWorkers();

		HTTP.Response res = HTTP.doRequest("GET",
				"http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/"
						+ java.net.URLEncoder.encode(row, "UTF-8") + "/" + java.net.URLEncoder.encode(column, "UTF-8"),
				null);
		return ((res != null) && (res.statusCode() == 200)) ? res.body() : null;
	}

	public boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException {
		if (!haveWorkers)
			downloadWorkers();

		HTTP.Response r = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/"
				+ tableName + "/" + java.net.URLEncoder.encode(row, "UTF-8"), null);
		return r.statusCode() == 200;
	}

	public int count(String tableName) throws IOException {
		if (!haveWorkers)
			downloadWorkers();

		int total = 0;
		for (WorkerEntry w : workers) {
			HTTP.Response r = HTTP.doRequest("GET", "http://" + w.address + "/count/" + tableName, null);
			if ((r != null) && (r.statusCode() == 200)) {
				String result = new String(r.body());
//      System.out.println("Total for "+w+": "+result);
				total += Integer.valueOf(result).intValue();
			}
		}
		return total;
	}

	public Iterator<Row> scan(String tableName) throws FileNotFoundException, IOException {
		return scan(tableName, null, null);
	}

	public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive)
			throws FileNotFoundException, IOException {
		if (!haveWorkers)
			downloadWorkers();

		return new KVSIterator(tableName, startRow, endRowExclusive);
	}
	
//	public boolean checkDictionary(String word) throws Exception {
//		if (!haveWorkers)
//			downloadWorkers();
//		
//		HTTP.Response r = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(word)).address + "/data/dictionary/" + java.net.URLEncoder.encode(word, "UTF-8"), null);
//		if (r.statusCode() == 200)
//			return true;
//		
//		return false;
//	}

	public static void main(String args[]) throws Exception {
		if (args.length < 2) {
			System.err.println("Syntax: client <master> get <tableName> <row> <column>");
			System.err.println("Syntax: client <master> put <tableName> <row> <column> <value>");
			System.err.println("Syntax: client <master> scan <tableName>");
			System.err.println("Syntax: client <master> rename <oldTableName> <newTableName>");
			System.exit(1);
		}

		KVSClient client = new KVSClient(args[0]);
		if (args[1].equals("put")) {
			if (args.length != 6) {
				System.err.println("Syntax: client <master> put <tableName> <row> <column> <value>");
				System.exit(1);
			}
			client.put(args[2], args[3], args[4], args[5].getBytes("UTF-8"));
		} else if (args[1].equals("get")) {
			if (args.length != 5) {
				System.err.println("Syntax: client <master> get <tableName> <row> <column>");
				System.exit(1);
			}
			byte[] val = client.get(args[2], args[3], args[4]);
			if (val == null)
				System.err.println("No value found");
			else
				System.out.write(val);
		} else if (args[1].equals("scan")) {
			if (args.length != 3) {
				System.err.println("Syntax: client <master> scan <tableName>");
				System.exit(1);
			}

			Iterator<Row> iter = client.scan(args[2], null, null);
//      while (iter.hasNext())
//        System.out.println(iter.next());
		} else if (args[1].equals("rename")) {
			if (args.length != 4) {
				System.err.println("Syntax: client <master> rename <oldTableName> <newTableName>");
				System.exit(1);
			}
			client.rename(args[2], args[3]);
		} else {
			System.err.println("Unknown command: " + args[1]);
			System.exit(1);
		}
	}
};