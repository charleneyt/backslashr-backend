package flame;

import java.util.*;
import java.io.*;
import kvs.*;

public interface FlameContext {
	public static KVSClient getKVS() {
		return FlameMaster.kvs;
	}

	public static void setKVS(String arg) {
		FlameMaster.kvs = new KVSClient(arg);
	}

	public interface RowToString extends Serializable {
		String op(Row r);
	};

	public interface RowMapToString extends Serializable {
		String op(Row r, HashSet<String> dict);
	};

	// When a job invokes output(), your solution should store the provided string
	// and return it in the body of the /submit response, if and when the job
	// terminates normally. If a job invokes output() more than once, the strings
	// should be concatenated. If a job never invokes output(), the body of the
	// /submit response should contain a message saying that there was no output.

	public void output(String s);

	// This function should return a FlameRDD that contains the strings in the
	// provided
	// List. It is okay for this method to run directly on the master; it does not
	// need to be parallelized.

	public FlameRDD parallelize(List<String> list) throws Exception;

	// This function should scan the table in the key-value store with the specified
	// name,
	// invoke the provided lambda with each Row of data from the KVS, and then
	// return
	// and RDD with all the strings that the lambda invocations returned. The lambda
	// is allowed to return null for certain Rows; when it does, no data should be
	// added to the RDD for these Rows. This method should run in parallel on all
	// the
	// workers, just like the RDD/PairRDD operations.

	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception;

	public FlameRDD indexFromTable(String tableName, RowMapToString lambda) throws Exception;

	public FlameRDD consolidateFromTable(String tableName, RowToString lambda) throws Exception;
}
