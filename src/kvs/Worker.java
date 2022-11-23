package kvs;

import static webserver.Server.*;
import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.StandardCopyOption.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import tools.HTTP;

public class Worker extends generic.Worker {
	final static byte[] LFbyte = new byte[] { (byte) 0x0a };
	final static String LF = "\n";

	Map<String, TreeMap<String, Row>> tables; // TODO: Ed post 372, row keys case sensitive and we sort with case
												// sensitive
	String directory;
	Map<String, BufferedOutputStream> streams;
	long lastRequestReceived;

	int workersCount;
	String nextHigherIpAndPort;
	String nextTwoHigherIpAndPort;

	String nextLowerKey;
	String nextTwoLowerKey;

	String nextLowerIpAndPort;
	String nextTwoLowerIpAndPort;

	public Worker(String id, int port, String masterAddr, String directory) {
		super(id, port);
		System.out.println("KVS Worker listening on " + port + " ... ");
		lastRequestReceived = System.currentTimeMillis();
		this.updateMasterIpAndPort(masterAddr);
		this.directory = directory;

		tables = new HashMap<>();
		streams = new ConcurrentHashMap<>();

		initializeTables();

		new Thread(new Flusher()).start();

		// EC 1
//		new Thread(new GarbageCollector()).start();

		// EC 2
		new Thread(new Replicator()).start();

		// EC 3
		new Thread(new Checker(1)).start();
		new Thread(new Checker(2)).start();
	}

	private void initializeTables() {
		File file = new File(directory);

		FileFilter filter = new FileFilter() {
			public boolean accept(File f) {
				return f.getName().endsWith(".table");
			}
		};

		File[] files = file.listFiles(filter);

		if (files == null) {
			return;
		}

		for (File tableFile : files) {
			try {
				String tableDir = tableFile.getName();
				FileInputStream input = new FileInputStream(tableFile);
				String tableName = tableDir.split(".table")[0];
				if (input.available() > 0) {
					addTable(tableName);

					while (input.available() > 0) {
						Row row = Row.readFrom(input);
						if (row != null) {
							tables.get(tableName).put(row.key(), row);
						}
					}
					streams.put(tableName, new BufferedOutputStream(new FileOutputStream(tableFile, true)));
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void updateRequestReceived() {
		lastRequestReceived = System.currentTimeMillis();
	}

	public void addTable(String tableName) {
		tables.put(tableName, new TreeMap<String, Row>());
	}

	private class Flusher implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(5000);
					for (BufferedOutputStream stream : streams.values()) {
						try {
							stream.flush();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	// EC 1 Every 10 seconds, check whether the worker has received any requests
	// during the past 10 seconds. If it has not, iterate through all the tables,
	// write a new log file for each table – initially under a different name – that
	// contains only entries for the rows that are currently in the table, and then
	// atomically replace the current log file for that table with the newly written
	// one.
	private class GarbageCollector implements Runnable {

		@Override
		public void run() {
			try {
				Thread.sleep(1000);
				while (true) {
					Thread.sleep(10000);
					if (lastRequestReceived + 10000 < System.currentTimeMillis()) {
						for (String tableName : tables.keySet()) {
							try {
								collectGarbage(tableName);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		void collectGarbage(String tableName) throws Exception {
			Path currFile = Paths.get(directory + "/" + tableName + ".table");
			Path tempFile = Paths.get(directory + "/" + tableName + ".table1");
			// write current row in the table to tempFile
			BufferedOutputStream newStream = new BufferedOutputStream(Files.newOutputStream(tempFile, CREATE, APPEND));
			for (Row row : tables.get(tableName).values()) {
				newStream.write(row.toByteArray());
				newStream.write(Worker.LFbyte);
			}
			// close the streams
			newStream.close();
			streams.get(tableName).close();

			// atomically replace the current log file
			Files.move(tempFile, currFile, ATOMIC_MOVE, REPLACE_EXISTING);

			// update the stream in streams
			streams.put(tableName, new BufferedOutputStream(Files.newOutputStream(currFile, CREATE, APPEND)));
		}

	}

	// EC 2 Replication: download the current list of workers from the master every
	// five seconds, whenever a worker that is currently responsible for a given key
	// receives a PUT for that key, it should forward the PUT to the two workers
	// with the next-higher IDs
	private class Replicator implements Runnable {

		@Override
		public void run() {
			try {
				Thread.sleep(2000);
				while (true) {
					workersCount = 0;

					// URL urlReq = new URL("http://" + masterAddrAndPort + "/workers");
					// HttpURLConnection conn = (HttpURLConnection) urlReq.openConnection();
					// conn.setDoInput(true);
					// conn.setRequestMethod("GET");
					// conn.setRequestProperty("Worker", id);
					// conn.setRequestProperty("Connection", "close");
					// BufferedReader in = new BufferedReader(new
					// InputStreamReader(conn.getInputStream()));

					// in.readLine();
					// Map<String, String> workersMap = new HashMap<>();
					// List<String> workerIds = new ArrayList<>();
					// String line;
					// while ((line = in.readLine()) != null){
					// String[] workerSplit = line.split(",");
					// workersMap.put(workerSplit[0], workerSplit[1]);
					// // no need to sort here, since master maintains and sends the sorted ver via
					// ConcurrentSkipListMap!
					// workerIds.add(workerSplit[0]);
					// }
					// in.close();
					// conn.disconnect();

					// workersCount = workersMap.size();
					// int currIndex = workerIds.indexOf(id);

					// if (currIndex != -1 && workersCount >= 3){
					// nextHigherIpAndPort = workersMap.get(workerIds.get( (currIndex + 1) %
					// workersCount ));
					// nextTwoHigherIpAndPort = workersMap.get(workerIds.get( (currIndex + 2) %
					// workersCount ));

					// nextLowerKey = workerIds.get( (currIndex - 1 + workersCount) % workersCount
					// );
					// nextTwoLowerKey = workerIds.get( (currIndex - 2 + workersCount) %
					// workersCount );

					// nextLowerIpAndPort = workersMap.get(nextLowerKey);
					// nextTwoLowerIpAndPort = workersMap.get(nextTwoLowerKey);
					// }

					String result = new String(
							HTTP.doRequest("GET", "http://" + masterAddrAndPort + "/workers", null).body());
					String[] pieces = result.split("\n");
					int numWorkers = Integer.parseInt(pieces[0]);
					Map<String, String> workersMap = new HashMap<>();
					List<String> workerIds = new ArrayList<>();

					if (numWorkers > 0) {
						for (int i = 0; i < numWorkers; i++) {
							String[] pcs = pieces[1 + i].split(",");
							workersMap.put(pcs[0], pcs[1]);
							workerIds.add(pcs[0]);
						}

						workersCount = numWorkers;
						int currIndex = workerIds.indexOf(id);

						if (currIndex != -1 && workersCount >= 3) {
							nextHigherIpAndPort = workersMap.get(workerIds.get((currIndex + 1) % workersCount));
							nextTwoHigherIpAndPort = workersMap.get(workerIds.get((currIndex + 2) % workersCount));

							nextLowerKey = workerIds.get((currIndex - 1 + workersCount) % workersCount);
							nextTwoLowerKey = workerIds.get((currIndex - 2 + workersCount) % workersCount);

							nextLowerIpAndPort = workersMap.get(nextLowerKey);
							nextTwoLowerIpAndPort = workersMap.get(nextTwoLowerKey);
						}
					}

					Thread.sleep(5000);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// EC 3 checker periodically generate list of current tables, and row keys and
	// hash of each
	private class Checker implements Runnable {
		int num; // mark the check is for next lower worker, or next two lower worker
		boolean wrapAround;
		String endKey;
		String startKeyInclusive;
		String ipAndPort;

		public Checker(int num) {
			this.num = num;
		}

		@Override
		public void run() {
			try {
				while (true) {
					if (workersCount >= 3) {
						if (num == 1) {
							startKeyInclusive = nextLowerKey;
							endKey = id;
							ipAndPort = nextLowerIpAndPort;
						} else {
							startKeyInclusive = nextTwoLowerKey;
							endKey = nextLowerKey;
							ipAndPort = nextTwoLowerIpAndPort;
						}

						wrapAround = startKeyInclusive.compareTo(endKey) > 0;

						// first connection: invoke on the next lower worker to get current list of
						// tables (in lowerTableList)
						// URL urlReq = new URL("http://" + ipAndPort + "/replica");
						// HttpURLConnection conn = (HttpURLConnection) urlReq.openConnection();
						// conn.setDoInput(true);
						// conn.setRequestMethod("GET");
						// conn.setRequestProperty("Worker", id);
						// conn.setRequestProperty("Connection", "close");
						// BufferedReader in = new BufferedReader(new
						// InputStreamReader(conn.getInputStream()));
						// in.readLine();
						// List<String> lowerTableList = new ArrayList<>();
						// String line;
						// while ((line = in.readLine()) != null){
						// lowerTableList.add(line);
						// }
						// in.close();
						// conn.disconnect();

						String result = new String(
								HTTP.doRequest("GET", "http://" + ipAndPort + "/replica", null).body());
						String[] pieces = result.split("\n");
						int numTables = Integer.parseInt(pieces[0]);
						List<String> lowerTableList = new ArrayList<>();

						if (numTables > 0) {
							for (int i = 1; i <= numTables; i++) {
								lowerTableList.add(pieces[i]);
							}
						}

						// then, invoke stream reads on each table to get the row keys and hash of each
						// row
						for (String tableName : lowerTableList) {
							TreeMap<String, Row> currTable = tables.get(tableName);
							if (!tables.containsKey(tableName)) {
								currTable = new TreeMap<String, Row>();
							}

							URL urlReq = new URL("http://" + ipAndPort + "/replica/"
									+ java.net.URLEncoder.encode(tableName, "UTF-8"));
							HttpURLConnection conn = (HttpURLConnection) urlReq.openConnection();
							conn.setDoInput(true);
							conn.setRequestMethod("GET");
							conn.setRequestProperty("Worker", id);
							// conn.setRequestProperty("Connection", "close");
							BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
							String line = in.readLine();

							boolean beginning = true;
							boolean end = false;
							List<String> rowToGet = new ArrayList<>();
							while ((line = in.readLine()) != null) {
								String[] lineSplit = line.split(",");
								if (wrapAround) {
									if (beginning) {
										if (lineSplit[0].compareTo(endKey) < 0) {
											if (!currTable.containsKey(lineSplit[0]) || currTable.get(lineSplit[0])
													.hashCode() != Integer.parseInt(lineSplit[1])) {
												rowToGet.add(lineSplit[0]);
											}
										} else {
											beginning = false; // end the initial read if not in the first range
										}
									} else if (end) {
										if (!currTable.containsKey(lineSplit[0]) || currTable.get(lineSplit[0])
												.hashCode() != Integer.parseInt(lineSplit[1])) {
											rowToGet.add(lineSplit[0]);
										}
									} else if (lineSplit[0].compareTo(startKeyInclusive) >= 0) {
										if (!currTable.containsKey(lineSplit[0]) || currTable.get(lineSplit[0])
												.hashCode() != Integer.parseInt(lineSplit[1])) {
											rowToGet.add(lineSplit[0]);
										}
										end = true;
									}
								} else {
									if (end)
										continue;
									else if (lineSplit[0].compareTo(startKeyInclusive) >= 0) {
										if (lineSplit[0].compareTo(endKey) < 0) {
											if (!currTable.containsKey(lineSplit[0]) || currTable.get(lineSplit[0])
													.hashCode() != Integer.parseInt(lineSplit[1])) {
												rowToGet.add(lineSplit[0]);
											}
										} else {
											end = true;
										}
									}
								}
							}
							in.close();
							// conn.disconnect();

							// now, we have the rowToGet list that saves all row keys in the table that we
							// need to get
							if (!tables.containsKey(tableName) && rowToGet.size() > 0) {
								// add currTable to table and create file and stream
								Path outputFile = Paths.get(directory + "/" + tableName + ".table");
								tables.put(tableName, currTable);
								streams.put(tableName,
										new BufferedOutputStream(Files.newOutputStream(outputFile, CREATE, APPEND)));
							}

							// update each row with a new request and save to table
							for (String rowName : rowToGet) {
								urlReq = new URL("http://" + ipAndPort + "/data/" + tableName + "/" + rowName);
								conn = (HttpURLConnection) urlReq.openConnection();
								conn.setDoInput(true);
								conn.setRequestMethod("GET");
								conn.setRequestProperty("Worker", id);
								// conn.setRequestProperty("Connection", "close");
								Row row = Row.readFrom(conn.getInputStream());
								if (row != null) {
									currTable.put(row.key(), row);
									streams.get(tableName).write(row.toByteArray());
									streams.get(tableName).write(Worker.LFbyte);
								}
								in.close();
								// conn.disconnect();
							}
						}
					}
					Thread.sleep(30000);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// Accept 3 command line args: 1) a port number for the worker, 2) a storage
	// directory, and 3) the IP and port of the master, separated by a colon (:).
	// When started, this application should look for a file called id in the
	// storage directory; if it exists, it should read the worker’s ID from this
	// file, otherwise it should pick an ID of five random lower-case letters and
	// write it to the file.
	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.out.println("Args should be 3!");
			return;
		}
		int port = Integer.valueOf(args[0]);
		if (!Files.isDirectory(Paths.get(args[1]))) {
			System.out.println("Given directory is not valid");
			return;
		}

		String masterAddr = args[2];
		if (!masterAddr.contains(":")) {
			System.out.println("Master address should be ip:port");
			return;
		}

		String id;
		Path dir = Paths.get(args[1]);
		if (!Files.isDirectory(dir)) {
			System.out.println("Args[1] should be a directory");
			return;
		}
		Path idFile = Paths.get(args[1] + "/id");
		if (Files.exists(idFile) && Files.isReadable(idFile)) {
			id = Files.readAllLines(idFile).get(0);
		} else {
			id = generateId(5);
			Files.write(idFile, id.getBytes());
		}

		port(port);
		Worker worker = new Worker(id, port, masterAddr, args[1]);

		// It should make a /ping request to the master every five seconds
		worker.startPingThread();

		// PUT /data/<T>/<R>/<C> should set column C in row R of table T to the
		// (possibly binary) data in the body of the request
		put("/data/:table/:row/:col", (req, res) -> {
			String tableName = req.params("table");
			String rowName = req.params("row");
			String colName = req.params("col");

			// EC 1
			worker.updateRequestReceived();

			// EC 2
			// if (!req.headers().contains("forwarded") && worker.workersCount >= 3){
			// URL urlReq = new URL("http://" + worker.nextHigherIpAndPort + "/data/" +
			// tableName + "/" + rowName + "/" + colName);
			// HttpURLConnection conn = (HttpURLConnection) urlReq.openConnection();
			// conn.setDoOutput(true);
			// conn.setRequestMethod("PUT");
			// conn.setRequestProperty("Host", worker.id);
			// conn.setRequestProperty("Forwarded", "true");
			// conn.setRequestProperty("Connection", "close");
			// conn.setRequestProperty("Content-Length",
			// String.valueOf(req.bodyAsBytes().length));
			// BufferedOutputStream out = new BufferedOutputStream(conn.getOutputStream());
			// out.write(req.bodyAsBytes());
			// out.close();
			// conn.getResponseCode();
			// conn.disconnect();

			// urlReq = new URL("http://" + worker.nextTwoHigherIpAndPort + "/data/" +
			// tableName + "/" + rowName + "/" + colName);
			// conn = (HttpURLConnection) urlReq.openConnection();
			// conn.setDoOutput(true);
			// conn.setRequestMethod("PUT");
			// conn.setRequestProperty("Host", worker.id);
			// conn.setRequestProperty("Forwarded", "true");
			// conn.setRequestProperty("Connection", "close");
			// conn.setRequestProperty("Content-Length",
			// String.valueOf(req.bodyAsBytes().length));
			// out = new BufferedOutputStream(conn.getOutputStream());
			// out.write(req.bodyAsBytes());
			// out.close();
			// conn.getResponseCode();
			// conn.disconnect();
			// }

			if (req.queryParams() != null && !req.queryParams().contains("forwarded") && worker.workersCount >= 3) {
				HTTP.doRequest("PUT",
						"http://" + worker.nextHigherIpAndPort + "/data/"
								+ java.net.URLEncoder.encode(tableName, "UTF-8") + "/"
								+ java.net.URLEncoder.encode(rowName, "UTF-8") + "/"
								+ java.net.URLEncoder.encode(colName, "UTF-8") + "?forwarded=true",
						req.bodyAsBytes());
				HTTP.doRequest("PUT",
						"http://" + worker.nextTwoHigherIpAndPort + "/data/"
								+ java.net.URLEncoder.encode(tableName, "UTF-8") + "/"
								+ java.net.URLEncoder.encode(rowName, "UTF-8") + "/"
								+ java.net.URLEncoder.encode(colName, "UTF-8") + "?forwarded=true",
						req.bodyAsBytes());
			}

			if (!worker.tables.containsKey(tableName)) {
				worker.addTable(tableName);
				Path outputFile = Paths.get(args[1] + "/" + tableName + ".table");
				worker.streams.put(tableName,
						new BufferedOutputStream(Files.newOutputStream(outputFile, CREATE, APPEND)));
			}

			Map<String, Row> currTable = worker.tables.get(tableName);
			Row row;
			if (!currTable.containsKey(rowName)) {
				row = new Row(rowName);
			} else {
				row = currTable.get(rowName);
			}
			row.put(colName, req.bodyAsBytes());
			currTable.put(rowName, row);
			worker.streams.get(tableName).write(row.toByteArray());
			worker.streams.get(tableName).write(Worker.LFbyte);
			return "OK";
		});

		// GET /data/<T>/<R>/<C> should return the data in column C of row R in table T
		// if the table, row, and column all exist; if not, it should return a 404 error
		get("/data/:table/:row/:col", (req, res) -> {
			String tableName = req.params("table");
			String rowName = req.params("row");
			String colName = req.params("col");

			// EC 1
			worker.updateRequestReceived();

			// check table, row, col all exist
			if (!worker.tables.containsKey(tableName)) {
				res.status(404, "Not Found");
				return "404 Not Found";
			} else if (!worker.tables.get(tableName).containsKey(rowName)) {
				res.status(404, "Not Found");
				return "404 Not Found";
			} else if (!worker.tables.get(tableName).get(rowName).columns().contains(colName)) {
				res.status(404, "Not Found");
				return "404 Not Found";
			}

			res.bodyAsBytes(worker.tables.get(tableName).get(rowName).getBytes(colName));
			return null;
		});

		// return HTML pages (content type text/html. The first route should return a
		// HTML table with a row for each data table on the worker; each row should
		// contain a) the name of the table – say, XXX – with a hyperlink to /view/XXX,
		// and b) the number of keys in the table.
		get("/", (req, res) -> {
			// EC 1
			worker.updateRequestReceived();

			res.type("text/html");
			StringBuilder sb = new StringBuilder();
			sb.append("<html>");
			sb.append("<style> tr, th, td {border: 1px solid black;}</style>");
			sb.append("<br>");
			sb.append("<table><tr><td>Table Name</td><td>Number of Row Keys</td></tr>");
			for (Map.Entry<String, TreeMap<String, Row>> tableEntry : worker.tables.entrySet()) {
				String link = "/view/" + tableEntry.getKey();
				sb.append("<tr><td><a href=\"" + link + "\">" + tableEntry.getKey() + "</td><td>"
						+ tableEntry.getValue().size() + "</td></tr>");
			}
			sb.append("</table>");
			return "<!doctype html><html><head><title>KVS Client - List of All Tables</title></head><body><div>KVS Client - List of All Tables</div>"
					+ sb.toString() + "</body></html>";
		});

		// return a HTML page with 10 rows of data; it should have one HTML row for each
		// row of data, one column for the row key, and one column for each column name
		// that appears at least once in those 10 rows. The cells should contain the
		// values in the relevant rows and columns, or be empty if the row does not
		// contain a column with that name. The rows should be sorted by column key. If
		// the data table contains more than 10 rows, the route should display the first
		// 10, and there should be a “Next” link at the end of the table that displays
		// another table with the next 10 rows. You may add query parameters to the
		// route for this purpose.
		get("/view/:table", (req, res) -> {
			String tableName = req.params("table");

			// EC 1
			worker.updateRequestReceived();

			if (worker.tables.containsKey(tableName)) {

				TreeMap<String, Row> currTable = worker.tables.get(tableName);

				if (currTable.size() < 1) {
					res.type("text/html");
					return "<!doctype html><html><head><title>KVS Client - Table " + tableName
							+ "</title></head><body><div>KVS Client - Table " + tableName + "</div>"
							+ "<table>Table is empty!</table>" + "</body></html>";
				}

				// get the sorted key in list
				List<String> rowsName = new ArrayList<>(currTable.navigableKeySet());

				// get the rows starting index, by default it's 0, or if query param has
				// startRow
				String startRow = rowsName.get(0);
				int startIndex = 0;
				if (req.queryParams() != null && req.queryParams().contains("startRow")) {
					startRow = req.queryParams("startRow");
					startIndex = rowsName.indexOf(startRow);
					if (startIndex == -1) {
						res.status(404, "Not Found");
						return "404 Not Found";
					}
				}
				// calculating endIndex to get a submap for better runtime
				int endIndex = Math.min(rowsName.size() - 1, startIndex + 9);
				NavigableMap<String, Row> subTable = currTable.subMap(startRow, true, rowsName.get(endIndex), true);

				// collect the column names in the 10 rows (or less if currently less than 10)
				Set<String> columnVals = new HashSet<>();
				for (Row row : subTable.values()) {
					columnVals.addAll(row.columns());
				}
				// inevitable work to sort the columns in current 10 rows!
				List<String> colsName = new ArrayList<>(columnVals);
				colsName.sort((a, b) -> a.compareTo(b));

				// start to make table!
				// first line
				StringBuilder sb = new StringBuilder();
				sb.append("<html>");
				sb.append("<style> tr, th, td {border: 1px solid black;}</style>");
				sb.append("<br>");
				sb.append("<table><tr><td>Row</td>");
				for (String col : colsName) {
					sb.append("<td>" + col + "</td>");
				}
				sb.append("</tr>");

				// every row, for up to 10 rows
				for (Row row : subTable.values()) {
					sb.append("<tr><td>" + row.key() + "</td>");
					for (String col : colsName) {
						if (row.columns().contains(col)) {
							sb.append("<td>" + row.get(col) + "</td>");
						} else {
							sb.append("<td></td>");
						}
					}
					sb.append("</tr>");
				}
				sb.append("</table>");

				// generate a Next link with startRow if we have more in the table
				if (endIndex + 1 < rowsName.size()) {
					String link = "/view/" + tableName + "?startRow=" + rowsName.get(endIndex + 1);
					sb.append("<div><a href=\"" + link + "\">" + "Next</div>");
				}

				res.type("text/html");
				return "<!doctype html><html><head><title>KVS Client - Table " + tableName
						+ "</title></head><body><div>KVS Client - Table " + tableName + "</div>" + sb.toString()
						+ "</body></html>";
			}
			res.status(404, "Not Found");
			return "404 Not Found";
		});

		// GET route for /data/XXX/YYY, where XXX is a table name and YYY is a row key.
		// If table XXX exists and contains a row with key YYY, the worker should
		// serialize this row using Row.toByteArray() and send it back in the body of
		// the response. If the table does not exist or does not contain a row with the
		// relevant key, it should return a 404 error code.
		get("/data/:table/:row", (req, res) -> {
			String tableName = req.params("table");
			String rowName = req.params("row");

			// EC 1
			worker.updateRequestReceived();

			// check table, row all exist
			if (worker.tables.containsKey(tableName) && worker.tables.get(tableName).containsKey(rowName)) {
				res.bodyAsBytes(worker.tables.get(tableName).get(rowName).toByteArray());
				return null;
			} else {
				res.status(404, "Not Found");
				return "404 Not Found";
			}
		});

		// iterate over all the local entries in table XXX, serialize each entry with
		// Row.toByteArray() and then send the entries back, each followed by a LF
		// character (ASCII code 10). After the last entry, there should be another LF
		// character to indicate the end of the stream.
		get("/data/:table", (req, res) -> {
			String tableName = req.params("table");

			// EC 1
			worker.updateRequestReceived();

			// check table all exist
			if (worker.tables.containsKey(tableName)) {
				// check for startRow and endRowExclusive in query params
				boolean hasStartRow = req.queryParams() != null && req.queryParams().contains("startRow");
				String startRow = "";
				if (hasStartRow) {
					startRow = req.queryParams("startRow");
				}
				boolean hasEndRow = req.queryParams() != null && req.queryParams().contains("endRowExclusive");
				String endRow = "";
				if (hasEndRow) {
					endRow = req.queryParams("endRowExclusive");
				}

				if (hasStartRow && hasEndRow && startRow.compareTo(endRow) > 0) {
					res.status(400, "Bad Request");
					return "400 Bad Request (startRow cannot be larger than endRowExclusive!)";
				}

				Collection<Row> workset = worker.tables.get(tableName).values();
				int rowCount = 0;
				boolean beginning = false;
				for (Row row : workset) {
					if (!hasStartRow || beginning || row.key().compareTo(startRow) >= 0) {
						beginning = true;
						if (!hasEndRow || row.key().compareTo(endRow) < 0) {
							res.write(row.toByteArray());
							res.write(Worker.LFbyte);
							rowCount++;
						} else {
							break;
						}
					}
				}
				res.write(Worker.LFbyte);
				// in case no row met the query
				if (rowCount == 0) {
					res.write(Worker.LFbyte);
				}
				return null;
			} else {
				res.status(404, "Not Found");
				return "404 Not Found";
			}
		});

		// the body will contain one or multiple rows, separated by a LF character. The
		// worker should read the rows one by one and insert them into table XXX;
		// existing entries with the same key should be overwritten.
		put("/data/:table", (req, res) -> {
			String tableName = req.params("table");

			// EC 1
			worker.updateRequestReceived();

			// EC 2
			// if (!req.headers().contains("forwarded") && worker.workersCount >= 3){
			// URL urlReq = new URL("http://" + worker.nextHigherIpAndPort + "/data/" +
			// tableName);
			// HttpURLConnection conn = (HttpURLConnection) urlReq.openConnection();
			// conn.setDoOutput(true);
			// conn.setRequestMethod("PUT");
			// conn.setRequestProperty("Host", worker.id);
			// conn.setRequestProperty("Forwarded", "true");
			// conn.setRequestProperty("Connection", "close");
			// conn.setRequestProperty("Content-Length",
			// String.valueOf(req.bodyAsBytes().length));
			// BufferedOutputStream out = new BufferedOutputStream(conn.getOutputStream());
			// out.write(req.bodyAsBytes());
			// out.close();
			// conn.getResponseCode();
			// conn.disconnect();

			// urlReq = new URL("http://" + worker.nextTwoHigherIpAndPort + "/data/" +
			// tableName);
			// conn = (HttpURLConnection) urlReq.openConnection();
			// conn.setDoOutput(true);
			// conn.setRequestMethod("PUT");
			// conn.setRequestProperty("Host", worker.id);
			// conn.setRequestProperty("Forwarded", "true");
			// conn.setRequestProperty("Connection", "close");
			// conn.setRequestProperty("Content-Length",
			// String.valueOf(req.bodyAsBytes().length));
			// out = new BufferedOutputStream(conn.getOutputStream());
			// out.write(req.bodyAsBytes());
			// out.close();
			// conn.getResponseCode();
			// conn.disconnect();
			// }

			if (req.queryParams() != null && !req.queryParams().contains("forwarded") && worker.workersCount >= 3) {
				HTTP.doRequest("PUT",
						"http://" + worker.nextHigherIpAndPort + "/data/"
								+ java.net.URLEncoder.encode(tableName, "UTF-8") + "?forwarded=true",
						req.bodyAsBytes());
				HTTP.doRequest("PUT",
						"http://" + worker.nextTwoHigherIpAndPort + "/data/"
								+ java.net.URLEncoder.encode(tableName, "UTF-8") + "?forwarded=true",
						req.bodyAsBytes());
			}

			if (!worker.tables.containsKey(tableName)) {
				worker.addTable(tableName);
				Path outputFile = Paths.get(args[1] + "/" + tableName + ".table");
				worker.streams.put(tableName,
						new BufferedOutputStream(Files.newOutputStream(outputFile, CREATE, APPEND)));
			}

			InputStream input = new ByteArrayInputStream(req.bodyAsBytes());
			while (input.available() > 0) {
				Row row = Row.readFrom(input);
				if (row != null) {
					worker.tables.get(tableName).put(row.key(), row);
					worker.streams.get(tableName).write(row.toByteArray());
					worker.streams.get(tableName).write(Worker.LFbyte);
				}
			}
			return "OK";
		});

		// the body will contain another name YYY, and the worker should rename table
		// XXX to YYY (and the corresponding log file from XXX.table to YYY.table. The
		// worker should return a 404 status if table XXX is not found, and a 409 status
		// if table YYY already exists.
		put("/rename/:table", (req, res) -> {
			String oldTable = req.params("table");
			String newTable = req.body();

			// EC 1
			worker.updateRequestReceived();

			// EC 2
			// if (!req.headers().contains("forwarded") && worker.workersCount >= 3){
			// URL urlReq = new URL("http://" + worker.nextHigherIpAndPort + "/rename/" +
			// tableName);
			// HttpURLConnection conn = (HttpURLConnection) urlReq.openConnection();
			// conn.setDoOutput(true);
			// conn.setRequestMethod("PUT");
			// conn.setRequestProperty("Host", worker.id);
			// conn.setRequestProperty("Forwarded", "true");
			// conn.setRequestProperty("Connection", "close");
			// conn.setRequestProperty("Content-Length",
			// String.valueOf(req.bodyAsBytes().length));
			// BufferedOutputStream out = new BufferedOutputStream(conn.getOutputStream());
			// out.write(req.bodyAsBytes());
			// out.close();
			// conn.getResponseCode();
			// conn.disconnect();

			// urlReq = new URL("http://" + worker.nextTwoHigherIpAndPort + "/rename/" +
			// tableName);
			// conn = (HttpURLConnection) urlReq.openConnection();
			// conn.setDoOutput(true);
			// conn.setRequestMethod("PUT");
			// conn.setRequestProperty("Host", worker.id);
			// conn.setRequestProperty("Forwarded", "true");
			// conn.setRequestProperty("Connection", "close");
			// conn.setRequestProperty("Content-Length",
			// String.valueOf(req.bodyAsBytes().length));
			// out = new BufferedOutputStream(conn.getOutputStream());
			// out.write(req.bodyAsBytes());
			// out.close();
			// conn.getResponseCode();
			// conn.disconnect();
			// }

			// if (!req.queryParams().contains("forwarded") && worker.workersCount >= 3){
			// HTTP.doRequest("PUT", "http://" + worker.nextHigherIpAndPort + "/rename/" +
			// java.net.URLEncoder.encode(oldTable, "UTF-8") + "?forwarded=true",
			// req.bodyAsBytes());
			// HTTP.doRequest("PUT", "http://" + worker.nextTwoHigherIpAndPort + "/rename/"
			// + java.net.URLEncoder.encode(oldTable, "UTF-8") + "?forwarded=true",
			// req.bodyAsBytes());
			// }

			if (!worker.tables.containsKey(oldTable)) {
				res.status(404, "Not Found");
				return "404 Not Found";
			}
			if (worker.tables.containsKey(newTable)) {
				res.status(409, "Conflict");
				return "409 Conflict";
			}

			Path oldFile = Paths.get(args[1] + "/" + oldTable + ".table");
			Path newFile = Paths.get(args[1] + "/" + newTable + ".table");

			// close the stream, then remove it
			worker.streams.get(oldTable).close();
			worker.streams.remove(oldTable);
			// rename the file (replace existing in case a .table exists but has not
			// associated table entry?) all tables should have .table and all .table should
			// have table. Not necessary for REPLACE_EXISTING
			Files.move(oldFile, newFile, ATOMIC_MOVE, REPLACE_EXISTING);
			// open the new stream for new table name
			worker.streams.put(newTable, new BufferedOutputStream(Files.newOutputStream(newFile, CREATE, APPEND)));

			// move the table from old to new key
			worker.tables.put(newTable, worker.tables.get(oldTable));
			// delete old entry
			worker.tables.remove(oldTable);

			return "OK";
		});

		// add the route to delete the table for worker
		put("/delete/:table", (req, res) -> {
			String tableToDelete = req.params("table");

			if (!worker.tables.containsKey(tableToDelete)) {
				res.status(404, "Not Found");
				return "404 Not Found";
			}

			Path fileToDelete = Paths.get(args[1] + "/" + tableToDelete + ".table");

			// close the stream, then remove it
			worker.streams.get(tableToDelete).close();
			worker.streams.remove(tableToDelete);

			// delete the file
			Files.deleteIfExists(fileToDelete);

			// delete old entry
			worker.tables.remove(tableToDelete);

			return "OK";
		});

		// If a table with this name exists, the body of the response should contain the
		// number of rows in that table (as an ASCII string); otherwise, you should send
		// a 404 error code.
		get("/count/:table", (req, res) -> {
			String tableName = req.params("table");

			// EC 1
			worker.updateRequestReceived();

			if (!worker.tables.containsKey(tableName)) {
				res.status(404, "Not Found");
				return "404 Not Found";
			} else {
				res.body(String.valueOf(worker.tables.get(tableName).size()));
				return null;
			}
		});

		// EC 3 return a list of table, with count at first, and table separated by LF
		get("/replica", (req, res) -> {

			// EC 1
			worker.updateRequestReceived();

			StringBuilder sb = new StringBuilder();
			sb.append(worker.tables.size() + Worker.LF);
			for (String table : worker.tables.keySet()) {
				sb.append(table + Worker.LF);
			}
			if (worker.tables.size() > 0) {
				sb.delete(sb.length() - 1, sb.length());
			}
			return sb.toString();
		});

		// EC 3 return row keys and hashCode for a table, separated by LF
		get("/replica/:table", (req, res) -> {
			String tableName = req.params("table");

			// EC 1
			worker.updateRequestReceived();

			if (!worker.tables.containsKey(tableName)) {
				return "0\n";
			}

			StringBuilder sb = new StringBuilder();
			sb.append(worker.tables.get(tableName).size() + Worker.LF);
			for (Map.Entry<String, Row> entry : worker.tables.get(tableName).entrySet()) {
				sb.append(entry.getKey() + "," + entry.getValue().hashCode() + Worker.LF);
			}
			if (worker.tables.size() > 0) {
				sb.delete(sb.length() - 1, sb.length());
			}
			return sb.toString();
		});
	}
}
