package flame;

import java.util.*;
import kvs.*;
import tools.*;
import tools.Partitioner.*;

public class FlamePairRDDImpl implements FlamePairRDD {
	FlameContextImpl context;
	public String tableName;
	String[] results;
	KVSClient kvs;
	Vector<Partition> latestAssignment;

	public FlamePairRDDImpl(FlameContextImpl context) {
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
	public List<FlamePair> collect() throws Exception {
		List<FlamePair> ret = new ArrayList<>();
		Iterator<Row> iter = kvs.scan(tableName, null, null);
		while (iter.hasNext()) {
			Row row = iter.next();
			if (row == null) {
				break;
			}
			for (String columnKey : row.columns()) {
				ret.add(new FlamePair(row.key(), row.get(columnKey)));
			}
		}
		return ret;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/rdd/foldByKey", Serializer.objectToByteArray(lambda),
				zeroElement, null);
		FlamePairRDDImpl ret = new FlamePairRDDImpl(context);
		ret.saveTable(outputTable);
		return ret;
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		kvs.rename(tableName, tableNameArg);
		tableName = tableNameArg;
	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/pairrdd/flatMap",
				Serializer.objectToByteArray(lambda), null, null);
		FlameRDDImpl ret = new FlameRDDImpl(context);
		ret.saveTable(outputTable);

		return ret;
	}

	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/pairrdd/flatMapToPair",
				Serializer.objectToByteArray(lambda), null, null);
		FlamePairRDDImpl ret = new FlamePairRDDImpl(context);
		ret.saveTable(outputTable);

		return ret;
	}

	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		String outputTable = context.invokeOperation(tableName, "/rdd/join", null, null, other.getTableName());
		FlamePairRDDImpl ret = new FlamePairRDDImpl(context);
		ret.saveTable(outputTable);

		return ret;
	}

	@Override
	public FlamePairRDD cogroup(FlamePairRDD R) throws Exception {
		String intermediaTable = context.invokeOperation(tableName, "/rdd/cogrouppre", null, null, R.getTableName());
		String outputTable = context.invokeOperation(intermediaTable, "/rdd/cogroup", null, null, null);
		FlamePairRDDImpl ret = new FlamePairRDDImpl(context);
		ret.saveTable(outputTable);

		return ret;
	}

}
