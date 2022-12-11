package flame;

import java.util.*;

import exceptions.*;
import tools.*;
import tools.HTTP.*;
import tools.Partitioner.*;

public class FlameContextImpl implements FlameContext {
	final static String LOWER_STRING = "abcdefghijklmnopqrstuvwxyz";
	final static String[] LOWER_KEYS = new String[]{"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
	StringBuilder sb;

	String output;
	FlameMaster master;
	public int masterPort;
	Set<String> tables;
	Vector<Partition> latestAssignment;

	public FlameContextImpl(String jarName, int masterPort) {
		super();
		output = "";
		this.masterPort = masterPort;
		master = new FlameMaster();
		tables = new HashSet<>();
		sb = new StringBuilder();
	}

	@Override
	public void output(String s) {
		output += s;
	}

	public String getOutput() {
		return output;
	}

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
		String tableName;
		while (tables.contains(tableName = generateTableName()))
			;
		tables.add(tableName);

		assignWorkers();

		for (int i = 0; i < list.size(); i++) {
			FlameContext.getKVS().put(tableName, Hasher.hash(String.valueOf(i)), "value", list.get(i).getBytes());
		}
		FlameRDDImpl ret = new FlameRDDImpl(this);
		ret.saveTable(tableName);
		return ret;
	}

	String generateTableName() {
		sb.setLength(0);
		Random rand = new Random();
		for (int i = 0; i < 5; i++) {
			sb.append(LOWER_STRING.charAt(rand.nextInt(26)));
		}
		sb.append(System.currentTimeMillis());
		return sb.toString();
	}

	void assignWorkers() throws Exception {
		// use partitioner (add kvs worker, then add flame worker, and call
		// assignPartitions())
		Partitioner partitioner = new Partitioner();
		int kvsWorkersCount = FlameContext.getKVS().numWorkers();
		if (kvsWorkersCount == 1) {
			partitioner.addKVSWorker(FlameContext.getKVS().getWorkerAddress(0), null, null);
		} else if (kvsWorkersCount > 1) {
			for (int i = 0; i < kvsWorkersCount - 1; i++) {
				partitioner.addKVSWorker(FlameContext.getKVS().getWorkerAddress(i),
						FlameContext.getKVS().getWorkerID(i), FlameContext.getKVS().getWorkerID(i + 1));
			}
			partitioner.addKVSWorker(FlameContext.getKVS().getWorkerAddress(kvsWorkersCount - 1),
					FlameContext.getKVS().getWorkerID(kvsWorkersCount - 1), null);
			partitioner.addKVSWorker(FlameContext.getKVS().getWorkerAddress(kvsWorkersCount - 1), null,
					FlameContext.getKVS().getWorkerID(0));
		}

		for (int i = 0; i < FlameMaster.getWorkers().size(); i++) {
			partitioner.addFlameWorker(FlameMaster.getWorkers().elementAt(i));
		}

		latestAssignment = partitioner.assignPartitions();

		// if assignment failed, raise exception
		if (latestAssignment == null){
			throw new UserDefinedException("There's no valid assignment!");
		}

		// 
	}

	public String invokeOperation(String inputTable, String operation, byte[] lambda, String zeroElement,
			String otherInputTable) throws Exception {
		// generate output table name
		String outputTableName;
		while (tables.contains(outputTableName = generateTableName()))
			;
		tables.add(outputTableName);

		assignWorkers();

		// divide the range into smaller ranges!
		// int totalRanges = 0;
		// Map<Integer, List<String>> dividedAssignment = new HashMap<>();
		// for (int i = 0; i < latestAssignment.size(); i++) {
		// 	Partition par = latestAssignment.elementAt(i);
		// 	List<String> ranges = new ArrayList<>();
		// 	int keyIdx = 0;
		// 	if (par.fromKey == null && par.toKeyExclusive == null){
		// 		ranges.add("null");
		// 		ranges.addAll(Arrays.asList(LOWER_KEYS));
		// 		ranges.add("null");
		// 		totalRanges += 27;
		// 	} else if (par.fromKey == null){
		// 		ranges.add("null");
		// 		int endIdx = LOWER_STRING.indexOf(par.toKeyExclusive.substring(0, 1));
		// 		while (keyIdx <= endIdx){
		// 			ranges.add(LOWER_KEYS[keyIdx++]);
		// 			totalRanges++;
		// 		}
		// 		if (par.toKeyExclusive.length() > 1){
		// 			ranges.add(par.toKeyExclusive);
		// 			totalRanges++;
		// 		}
		// 	} else if (par.toKeyExclusive == null){
		// 		ranges.add(par.fromKey);
		// 		totalRanges++;
		// 		keyIdx = LOWER_STRING.indexOf(par.fromKey.substring(0, 1))+1;
		// 		while (keyIdx < 26){
		// 			ranges.add(LOWER_KEYS[keyIdx++]);
		// 			totalRanges++;
		// 		}
		// 		ranges.add("null");
		// 	} else {
		// 		ranges.add(par.fromKey);
		// 		keyIdx = LOWER_STRING.indexOf(par.fromKey.substring(0, 1))+1;
		// 		int endIdx = LOWER_STRING.indexOf(par.toKeyExclusive.substring(0, 1));
				
		// 		while (keyIdx <= endIdx){
		// 			ranges.add(LOWER_KEYS[keyIdx++]);
		// 			totalRanges++;
		// 		}
		// 		if (par.toKeyExclusive.length() > 1){
		// 			ranges.add(par.toKeyExclusive);
		// 			totalRanges++;
		// 		}
		// 	}
		// 	// System.out.println("from key: "+par.fromKey+" to key: "+par.toKeyExclusive);
		// 	// for (String range : ranges){
		// 	// 	System.out.println("Flameworker "+par.assignedFlameWorker+" assigned range: "+range);
		// 	// }
		// 	dividedAssignment.put(i, ranges);
		// }

		// else send out work to workers with given work load
		Thread threads[] = new Thread[latestAssignment.size()];
		String results[] = new String[latestAssignment.size()];
		// Thread threads[] = new Thread[totalRanges];
		// String results[] = new String[totalRanges];

		String[] kvsMaster = FlameContext.getKVS().getMaster().split(":");
		String zeroElementUrl = zeroElement == null ? ""
				: ("&zeroElement=" + java.net.URLEncoder.encode(zeroElement, "UTF-8"));
		String secondTableUrl = otherInputTable == null ? ""
				: ("&secondTable=" + java.net.URLEncoder.encode(otherInputTable, "UTF-8"));
		final String fixedUrl = operation + "?input="
		+ java.net.URLEncoder.encode(inputTable, "UTF-8") + "&output="
		+ java.net.URLEncoder.encode(outputTableName, "UTF-8") + "&kvsMasterIp=" + kvsMaster[0]
		+ "&kvsMasterPort=" + kvsMaster[1] + zeroElementUrl + secondTableUrl;

		int operationCount = 0;
		for (int i = 0; i < latestAssignment.size(); i++) {
			Partition par = latestAssignment.elementAt(i);

			String fromKeyUrl = par.fromKey == null ? ""
					: ("&startKey=" + java.net.URLEncoder.encode(par.fromKey, "UTF-8"));
			String toKeyExclusiveUrl = par.toKeyExclusive == null ? ""
					: ("&toKeyExclusive=" + java.net.URLEncoder.encode(par.toKeyExclusive, "UTF-8"));

			final String url = "http://" + par.assignedFlameWorker + operation + "?input="
					+ java.net.URLEncoder.encode(inputTable, "UTF-8") + "&output="
					+ java.net.URLEncoder.encode(outputTableName, "UTF-8") + "&kvsMasterIp=" + kvsMaster[0]
					+ "&kvsMasterPort=" + kvsMaster[1] + fromKeyUrl + toKeyExclusiveUrl + zeroElementUrl
					+ secondTableUrl;

			final int j = i;
			threads[i] = new Thread(operation + "#" + (i + 1)) {
				public void run() {
					try {
						Response r = HTTP.doRequest("POST", url, lambda);
						if (r.statusCode() != 200) {
							results[j] = "Worker" + j + "Failed: " + new String(r.body());
						} else {
							results[j] = "OK";
						}
					} catch (Exception e) {
						results[j] = "Worker" + j + "Exception: " + e;
						e.printStackTrace();
					}
				}
			};
			threads[i].start();

			// final String preUrl = "http://" + par.assignedFlameWorker + fixedUrl;
			// List<String> ranges = dividedAssignment.get(i);
			// for (int j = 0; j < ranges.size()-1; j++){
			// 	String fromKeyUrl = "null".equals(ranges.get(j)) ? ""
			// 		: ("&startKey=" + java.net.URLEncoder.encode(ranges.get(j), "UTF-8"));
			// 	String toKeyExclusiveUrl = "null".equals(ranges.get(j+1)) ? ""
			// 		: ("&toKeyExclusive=" + java.net.URLEncoder.encode(ranges.get(j+1), "UTF-8"));

			// 	final String url = preUrl + fromKeyUrl + toKeyExclusiveUrl;

			// 	// System.out.println(operationCount+" link: "+url);

			// 	final int num = operationCount;
			// 	threads[operationCount] = new Thread(operation + "#" + (operationCount+1)) {
			// 		public void run() {
			// 			try {
			// 				Response r = HTTP.doRequest("POST", url, lambda);
			// 				if (r.statusCode() != 200) {
			// 					results[num] = "Worker" + num + "Failed: " + new String(r.body());
			// 				} else {
			// 					results[num] = "OK";
			// 				}
			// 			} catch (Exception e) {
			// 				results[num] = "Worker" + num + "Exception: " + e;
			// 				e.printStackTrace();
			// 			}
			// 		}
			// 	};
			// 	threads[operationCount++].start();
			// }
		}
		// wait until all threads finished
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ie) {
			}
		}

		for (int i = 0; i < results.length; i++) {
			if (!"OK".equals(results[i])) {
				System.out.println(results[i]);
				// throw new UserDefinedException(results[i]);
			}
		}

		return outputTableName;
	}

	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		String outputTable = invokeOperation(tableName, "/rdd/fromTable", Serializer.objectToByteArray(lambda), null,
				null);

		FlameRDDImpl ret = new FlameRDDImpl(this);
		ret.saveTable(outputTable);
		return ret;
	}
	
	@Override
	public FlameRDD indexFromTable(String tableName, RowMapToString lambda) throws Exception {
		String outputTable = invokeOperation(tableName, "/rdd/indexFromTable", Serializer.objectToByteArray(lambda), null,
				null);

		FlameRDDImpl ret = new FlameRDDImpl(this);
		ret.saveTable(outputTable);
		return ret;
	}
	
	@Override
	public FlameRDD consolidateFromTable(String tableName, RowToString lambda) throws Exception {
		String outputTable = invokeOperation(tableName, "/rdd/consolidateFromTable", Serializer.objectToByteArray(lambda), null,
				null);

		FlameRDDImpl ret = new FlameRDDImpl(this);
		ret.saveTable(outputTable);
		return ret;
	}
}
