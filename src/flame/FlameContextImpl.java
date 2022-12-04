package flame;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import exceptions.*;
import tools.*;
import tools.HTTP.*;
import tools.Partitioner.*;

public class FlameContextImpl implements FlameContext {
	final static String LOWER_STRING = "abcdefghijklmnopqrstuvwxyz";
	StringBuilder sb;

	String output;
	FlameMaster master;
	public int masterPort;
	Set<String> tables;
	Vector<Partition> latestAssignment;
	boolean debugMode = false;

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
		if (latestAssignment == null)
			throw new UserDefinedException("There's no valid assignment!");
	}

	public String invokeOperation(String inputTable, String operation, byte[] lambda, String zeroElement,
			String otherInputTable) throws Exception {
		// generate output table name
		String outputTableName;
		while (tables.contains(outputTableName = generateTableName()))
			;
		tables.add(outputTableName);

		assignWorkers();

		// else send out work to workers with given work load
		Thread threads[] = new Thread[latestAssignment.size()];
		String results[] = new String[latestAssignment.size()];

		String[] kvsMaster = FlameContext.getKVS().getMaster().split(":");
		String zeroElementUrl = zeroElement == null ? ""
				: ("&zeroElement=" + java.net.URLEncoder.encode(zeroElement, "UTF-8"));
		String secondTableUrl = otherInputTable == null ? ""
				: ("&secondTable=" + java.net.URLEncoder.encode(otherInputTable, "UTF-8"));
//		System.out.println("latestAssignment.size()" + latestAssignment.size());
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
//					long startTime = System.currentTimeMillis();
					try {
//						System.out.println("invokeOperation url " + url);
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
//					long endTime = System.currentTimeMillis();
					
//					FileWriter fw;
//					try {
//						fw = new FileWriter("FlameContext_log", true);
//						fw.write("thread " + operation + "#" + " start time: " + startTime + ", end time: " + endTime + ", took " + (endTime - startTime) + " to run \n");
//						fw.close();
//					} catch (IOException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}

				}
			};
			threads[i].start();
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
				if (debugMode) {
					FileWriter fw = new FileWriter("FlameContextImpl_error_log", true);
					fw.write(results[i]);
					fw.flush();					
				}
//				throw new UserDefinedException(results[i]);
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
}
