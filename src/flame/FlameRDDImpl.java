package flame;

import flame.FlamePairRDD.*;
import kvs.*;
import tools.*;
import tools.Partitioner.*;
import java.nio.*;
import java.util.*;

public class FlameRDDImpl implements FlameRDD {
	FlameContextImpl context;
	public String tableName;
	String[] results;
	KVSClient kvs;
	Vector<Partition> latestAssignment;

	public FlameRDDImpl(FlameContextImpl context) {
		this.context = context;
		tableName = null;
		results = null;
		kvs = FlameContext.getKVS();
		latestAssignment = context.latestAssignment;
	}

	public void saveTable(String tableName) {
		this.tableName = tableName;
	}

	public void saveResults(String[] results) {
		this.results = results;
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public List<String> collect() throws Exception {
		List<String> ret = new ArrayList<>();
		Iterator<Row> iter = kvs.scan(tableName, null, null);
		while (iter.hasNext()) {
			Row row = iter.next();
			if (row == null) {
				break;
			}
			ret.add(row.get("value"));
		}
		return ret;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/rdd/flatMap", Serializer.objectToByteArray(lambda),
				null, null);
		FlameRDDImpl ret = new FlameRDDImpl(context);
		ret.saveTable(outputTable);

		return ret;
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/rdd/mapToPair", Serializer.objectToByteArray(lambda),
				null, null);
		FlamePairRDDImpl ret = new FlamePairRDDImpl(context);
		ret.saveTable(outputTable);
		return ret;
	}

	public FlameRDD intersection(FlameRDD R) throws Exception {
		// read two table, and save
		String intermediateTable = context.invokeOperation(tableName, "/rdd/combine", null, null, R.getTableName());

		String outputTable = context.invokeOperation(intermediateTable, "/rdd/intersection", null, null, null);
		FlameRDDImpl ret = new FlameRDDImpl(context);
		ret.saveTable(outputTable);
		return ret;
	}

	public FlameRDD sample(double f) throws Exception {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putDouble(f);

		String outputTable = context.invokeOperation(tableName, "/rdd/sample", bytes, null, null);

		FlameRDDImpl ret = new FlameRDDImpl(context);
		ret.saveTable(outputTable);
		return ret;
	}

	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/rdd/groupBy", Serializer.objectToByteArray(lambda),
				null, null);

		FlamePairRDDImpl ret = new FlamePairRDDImpl(context);
		ret.saveTable(outputTable);
		return ret;
	}

	@Override
	public int count() throws Exception {
		return kvs.count(tableName);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		kvs.rename(tableName, tableNameArg);
		tableName = tableNameArg;
	}

	@Override
	public FlameRDD distinct() throws Exception {
		String outputTable = context.invokeOperation(tableName, "/rdd/distinct", null, null, null);
		FlameRDDImpl ret = new FlameRDDImpl(context);
		ret.saveTable(outputTable);

		return ret;
	}

	@Override
	public Vector<String> take(int num) throws Exception {
		Vector<String> ret = new Vector<>();
		num = Math.min(num, count());
		while (num > 0) {
			Iterator<Row> iter = kvs.scan(tableName, null, null);
			while (num > 0 && iter.hasNext()) {
				Row row = iter.next();
				if (row == null) {
					break;
				}
				ret.add(row.get("value"));
				num--;
			}
		}
		return ret;
	}

	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/rdd/fold", Serializer.objectToByteArray(lambda),
				zeroElement, null);

		String accumulator = null;

		for (Partition par : latestAssignment) {
			String distinguisher = par.fromKey != null ? par.fromKey : "null";
			distinguisher += par.toKeyExclusive != null ? par.toKeyExclusive : "null";
			accumulator = lambda.op(accumulator == null ? zeroElement : accumulator,
					new String(kvs.get(outputTable, distinguisher, "value")));
		}

		return accumulator == null ? zeroElement : accumulator;
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/rdd/flatMapToPair",
				Serializer.objectToByteArray(lambda), null, null);
		FlamePairRDDImpl ret = new FlamePairRDDImpl(context);
		ret.saveTable(outputTable);

		return ret;
	}

	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/rdd/filter", Serializer.objectToByteArray(lambda),
				null, null);
		FlameRDDImpl ret = new FlameRDDImpl(context);
		ret.saveTable(outputTable);

		return ret;
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/rdd/mapPartitions",
				Serializer.objectToByteArray(lambda), null, null);
		FlameRDDImpl ret = new FlameRDDImpl(context);
		ret.saveTable(outputTable);

		return ret;
	}
}
