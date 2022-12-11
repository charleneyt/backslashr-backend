package flame;

import static webserver.Server.*;

import flame.FlameContext.*;
import flame.FlameRDD.*;
import flame.FlamePairRDD.*;
import generic.Worker;
import tools.*;
import kvs.*;
import webserver.*;
import java.util.*;
import java.nio.*;
import java.awt.datatransfer.SystemFlavorMap;
import java.io.*;

class FlameWorker extends Worker {
	public FlameWorker(String id, int port, String masterAddr) {
		super(id, port);
		this.updateMasterIpAndPort(masterAddr);
	}

	final static String LOWER_STRING = "abcdefghijklmnopqrstuvwxyz";
	final static String OK_STRING = "OK";
	final static String VALUE_STRING = "value";
	static HashSet<String> dictionary;

	public static void main(String args[]) {
		if (args.length != 2) {
			System.err.println("Syntax: FlameWorker <port> <masterIP:port>");
			System.exit(1);
		}

		int port = Integer.parseInt(args[0]);
		System.out.println("Flame Worker listening on " + port + " ... ");
		String server = args[1];
		FlameWorker fw = new FlameWorker(generateId(5), port, server);
		fw.startPingThread();

		final File myJAR = new File("__worker" + port + "-current.jar");

		// load up dictionary
		dictionary = new HashSet<String>();
		File file = new File("./filtered_stop_words.txt");
		FileReader fr;
		try {
			fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line;
			while ((line = br.readLine()) != null) {
				dictionary.add(line.toLowerCase().trim());
			}
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		port(port);

		post("/useJAR", (request, response) -> {
			FileOutputStream fos = new FileOutputStream(myJAR);
			fos.write(request.bodyAsBytes());
			fos.close();
			return FlameWorker.OK_STRING;
		});

		post("/rdd/flatMap", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}

					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						Iterable<String> it = lambda.op(row.get(FlameWorker.VALUE_STRING));
						if (it != null) {
							int seq = 0;
							for (String s : it) {
								// System.out.println("put val is: " + s);
								kvs.put(qParamsStrings[1], Hasher.hash(row.key() + String.valueOf(seq++)),
										FlameWorker.VALUE_STRING, s.getBytes());
							}
						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/mapToPair", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}

					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						FlamePair it = lambda.op(row.get(FlameWorker.VALUE_STRING));
						if (it != null) {
							kvs.put(qParamsStrings[1], it.a, row.key(), it.b.getBytes());
						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/foldByKey", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);
			String zeroElement = req.queryParams().contains("zeroElement") ? req.queryParams("zeroElement") : "";

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}
					String accumulator = null;
					for (String colName : row.columns()) {
						accumulator = lambda.op(accumulator == null ? zeroElement : accumulator, row.get(colName));
					}
					kvs.put(qParamsStrings[1], row.key(), FlameWorker.VALUE_STRING,
							accumulator == null ? zeroElement.getBytes() : accumulator.getBytes());
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/combine", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}
					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						String colVal = row.get(FlameWorker.VALUE_STRING);
						kvs.put(qParamsStrings[1], colVal, "left_value", colVal.getBytes());
					}
				}
			}

			String secondTable = req.queryParams().contains("secondTable") ? req.queryParams("secondTable") : "";
			iter = kvs.scan(secondTable, qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}
					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						String colVal = row.get(FlameWorker.VALUE_STRING);
						kvs.put(qParamsStrings[1], colVal, "right_value", colVal.getBytes());
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/intersection", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}
					if (row.columns().contains("left_value") && row.columns().contains("right_value")) {
						kvs.put(qParamsStrings[1], row.key(), FlameWorker.VALUE_STRING, row.key().getBytes());
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/sample", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			double f = ByteBuffer.wrap(req.bodyAsBytes()).getDouble();
			KVSClient kvs = new KVSClient(qParamsStrings[2]);
			Random rand = new Random();
			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}

					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						if (rand.nextDouble() < f) {
							kvs.put(qParamsStrings[1], row.key(), FlameWorker.VALUE_STRING,
									row.get(FlameWorker.VALUE_STRING).getBytes());
						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/groupBy", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			StringToString lambda = (StringToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}

					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						String val = row.get(FlameWorker.VALUE_STRING);
						String group = lambda.op(val);
						if (group != null) {
							kvs.put(qParamsStrings[1], group, row.key(), val.getBytes());
						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/fromTable", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			RowToString lambda = (RowToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					FileWriter fw1 = new FileWriter("flameworker_fromtable_log", true);
					fw1.write(row.key() + "\n");
					fw1.close();
					if (row == null) {
						break;
					}

					String s = lambda.op(row);
					if (s != null) {
						kvs.put(qParamsStrings[1], row.key(), FlameWorker.VALUE_STRING, s.getBytes());
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/indexFromTable", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			RowMapToString lambda = (RowMapToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);
			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				int counter = 0;
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}

					String s = lambda.op(row, dictionary);
					if (s != null) {
						counter++;
						kvs.put(qParamsStrings[1], row.key(), FlameWorker.VALUE_STRING, s.getBytes());
					}
					// clean garbage and print heart beat for every 100 lines
					if (counter % 100 == 0) {
						System.out.println("Indexed " + counter + " rows");
//						try {
//							kvs.clean("index_imm");
//							System.out.println("collecting garbage...");
//						} catch (IOException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/consolidateFromTable", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			String inputTableName = qParamsStrings[0];
			String outputTableName = qParamsStrings[1];
			String kvsMaster = qParamsStrings[2];
			String startKey = qParamsStrings[3];
			String toKeyExclusive = qParamsStrings[4];
			System.out.println("start key:  " + startKey + ", end key: " + toKeyExclusive);

			RowToString lambda = (RowToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(kvsMaster);

			Iterator<Row> iter = kvs.scanRaw(inputTableName, startKey, toKeyExclusive);
			if (iter == null) {
				return FlameWorker.OK_STRING;
			}

			String lastRowKey = "";
			StringBuilder sb = new StringBuilder();
			int counter = 0;
			while (iter.hasNext()) {
				if (counter % 5000 == 0) {
					System.out.println(counter + "processed");
					
				}
				counter++;
				Row row = iter.next();
				if (row == null) {
					break;
				}

				if (row.key().equals(lastRowKey)) {
					sb.append("," + row.get("value"));
				} else {
					if (lastRowKey.length() > 0) {
						Row rowToWrite = new Row(lastRowKey);
						rowToWrite.put("value", sb.toString());
						kvs.putRow("index", rowToWrite);
					}

					sb.setLength(0);
					sb.append(row.get("value"));
				}

				lastRowKey = row.key();
			}

			Row row = new Row(lastRowKey);
			row.put("value", sb.toString());
			kvs.putRow("index", row);
			
			return FlameWorker.OK_STRING;
		});

		post("/rdd/flatMapToPair", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			StringToPairIterable lambda = (StringToPairIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}

					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						Iterable<FlamePair> it = lambda.op(row.get(FlameWorker.VALUE_STRING));
						if (it != null) {
							int seq = 0;
							for (FlamePair p : it) {
								if (!"".equals(p._1())) {
									kvs.put(qParamsStrings[1], p._1(), row.key() + seq++, p._2().getBytes());
								}
							}
						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/pairrdd/flatMap", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			PairToStringIterable lambda = (PairToStringIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}

					for (String colName : row.columns()) {
						FlamePair pair = new FlamePair(row.key(), row.get(colName));
						Iterable<String> it = lambda.op(pair);
						if (it != null) {
							int seq = 0;
							for (String s : it) {
								kvs.put(qParamsStrings[1], row.key() + colName + String.valueOf(seq++),
										FlameWorker.VALUE_STRING, s.getBytes());
							}
						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/pairrdd/flatMapToPair", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			PairToPairIterable lambda = (PairToPairIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}

					for (String colName : row.columns()) {
						FlamePair pair = new FlamePair(row.key(), row.get(colName));
						Iterable<FlamePair> it = lambda.op(pair);
						if (it != null) {
							int seq = 0;
							for (FlamePair p : it) {
								if (!"".equals(p._1())) {
									kvs.put(qParamsStrings[1], p._1(), row.key() + seq++, p._2().getBytes());
								}
							}
						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/distinct", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}
					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						String s = row.get(FlameWorker.VALUE_STRING);
						if (s != null) {
							kvs.put(qParamsStrings[1], s, FlameWorker.VALUE_STRING, s.getBytes());
						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/join", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);
			String secondTable = req.queryParams().contains("secondTable") ? req.queryParams("secondTable") : "";

			if (!secondTable.equals("")) {
				Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
				if (iter != null) {
					while (iter.hasNext()) {
						Row rowL = iter.next();
						if (rowL == null) {
							break;
						}
						Row rowR;
						if (!kvs.existsRow(secondTable, rowL.key())
								|| (rowR = kvs.getRow(secondTable, rowL.key())) == null) {
							continue;
						}
						for (String colLName : rowL.columns()) {
							for (String colRName : rowR.columns()) {
								kvs.put(qParamsStrings[1], rowL.key(), Hasher.hash(colLName + "|||" + colRName),
										(rowL.get(colLName) + "," + rowR.get(colRName)).getBytes());
							}
						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/fold", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
//
//			KVSClient kvs = new KVSClient(qParamsStrings[2]);
//			String zeroElement = req.queryParams().contains("zeroElement") ? req.queryParams("zeroElement") : "";
//			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
//			if (iter != null) {
//				while (iter.hasNext()) {
//					String sum = zeroElement;
//					Row row = iter.next();
//					for (String col : row.columns()) {
//						sum = lambda.op(sum, row.get(col));
//						kvs.put(qParamsStrings[1], row.key(), "Accumulator", sum.getBytes());
//					}
//				}
//			}

			KVSClient kvs = new KVSClient(qParamsStrings[2]);
			String zeroElement = req.queryParams().contains("zeroElement") ? req.queryParams("zeroElement") : "";

			String accumulator = null;
			String distinguisher = qParamsStrings[3] != null ? qParamsStrings[3] : "null";
			distinguisher += qParamsStrings[4] != null ? qParamsStrings[4] : "null";

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}
					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						String s = row.get(FlameWorker.VALUE_STRING);
						if (s != null) {
							accumulator = lambda.op(accumulator == null ? zeroElement : accumulator, s);
						}
					}
				}
			}

			// check for the special circumstance if a worker was called twice
			// for example, the highest id responsible for beginning and end range
			kvs.put(qParamsStrings[1], distinguisher, FlameWorker.VALUE_STRING,
					accumulator == null ? zeroElement.getBytes() : accumulator.getBytes());

			return FlameWorker.OK_STRING;
		});

		// EC 1
		post("/rdd/filter", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			StringToBoolean lambda = (StringToBoolean) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}

					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						if (lambda.op(row.get(FlameWorker.VALUE_STRING))) {
							kvs.put(qParamsStrings[1], row.key(), FlameWorker.VALUE_STRING,
									row.get(FlameWorker.VALUE_STRING).getBytes());
						}
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		// EC 2
		post("/rdd/mapPartitions", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);
			IteratorToIterator lambda = (IteratorToIterator) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);
			List<String> in = new LinkedList<>();
			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}

					if (row.columns().contains(FlameWorker.VALUE_STRING)) {
						in.add(row.get(FlameWorker.VALUE_STRING));
					}
				}
			}

			String distinguisher = qParamsStrings[3] != null ? qParamsStrings[3] : "null";
			distinguisher += "|" + qParamsStrings[4] != null ? qParamsStrings[4] : "null";
			Iterator<String> out = lambda.op(in.iterator());
			if (out != null) {
				int seq = 0;
				while (out.hasNext()) {
					String o = out.next();
					kvs.put(qParamsStrings[1], distinguisher + seq++, FlameWorker.VALUE_STRING, o.getBytes());
				}
			}

			return FlameWorker.OK_STRING;
		});

		// EC 3
		post("/rdd/cogrouppre", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);
			String secondTable = req.queryParams().contains("secondTable") ? req.queryParams("secondTable") : "";

			String comma = ",";
			StringBuilder sb = new StringBuilder();

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}
					sb.setLength(0);
					for (String colName : row.columns()) {
						sb.append(row.get(colName) + comma);
					}
					if (sb.charAt(sb.length() - 1) == ',') {
						sb.delete(sb.length() - 1, sb.length());
					}
					kvs.put(qParamsStrings[1], row.key(), "left", sb.toString().getBytes());
				}
			}

			if (!secondTable.equals("")) {
				iter = kvs.scan(secondTable, qParamsStrings[3], qParamsStrings[4]);
				if (iter != null) {
					while (iter.hasNext()) {
						Row row = iter.next();
						if (row == null) {
							break;
						}
						sb.setLength(0);
						for (String colName : row.columns()) {
							sb.append(row.get(colName) + comma);
						}
						if (sb.charAt(sb.length() - 1) == ',') {
							sb.delete(sb.length() - 1, sb.length());
						}
						kvs.put(qParamsStrings[1], row.key(), "right", sb.toString().getBytes());
					}
				}
			}

			return FlameWorker.OK_STRING;
		});

		post("/rdd/cogroup", (req, res) -> {
			String[] qParamsStrings = parseRequestQueryParams(req);

			KVSClient kvs = new KVSClient(qParamsStrings[2]);

			String leftBracket = "[";
			String rightBracket = "]";
			String comma = ",";
			StringBuilder sb = new StringBuilder();

			Iterator<Row> iter = kvs.scan(qParamsStrings[0], qParamsStrings[3], qParamsStrings[4]);
			if (iter != null) {
				while (iter.hasNext()) {
					Row row = iter.next();
					if (row == null) {
						break;
					}
					sb.setLength(0);
					sb.append(leftBracket);
					if (row.columns().contains("left")) {
						sb.append(row.get("left"));
					}
					sb.append(rightBracket + comma + leftBracket);
					if (row.columns().contains("right")) {
						sb.append(row.get("right"));
					}
					sb.append(rightBracket);
					kvs.put(qParamsStrings[1], row.key(), FlameWorker.VALUE_STRING, sb.toString().getBytes());
				}
			}

			return FlameWorker.OK_STRING;
		});

	}

	public static String[] parseRequestQueryParams(Request req) {
		String[] ret = new String[5];
		ret[0] = req.queryParams("input");
		ret[1] = req.queryParams("output");
		ret[2] = req.queryParams("kvsMasterIp") + ":" + req.queryParams("kvsMasterPort");
		ret[3] = req.queryParams().contains("startKey") ? req.queryParams("startKey") : null;
		ret[4] = req.queryParams().contains("toKeyExclusive") ? req.queryParams("toKeyExclusive") : null;

		return ret;
	}
}
