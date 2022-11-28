package jobs;

import java.util.ArrayList;
import java.util.List;

import flame.FlameContext;
import kvs.KVSClient;
import tools.Hasher;

public class AddTable {
	public static void run(FlameContext ctx, String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Please specify name of the table and column of the table you want to add.");
		}
		
		KVSClient kvs = FlameContext.getKVS();
		String tableName = args[0];
		String colName = args[1];
		
		List<String> values = new ArrayList<>();
		values.add("http*://*.twitter.*");
		values.add("http*://*.youtube.*");
		values.add("http*://*.instagram.*");
		
		for (String value : values) {
			kvs.put(tableName, Hasher.hash(value), colName, value.getBytes());
		}
		
		System.out.println("Saved table " + tableName);
	}
}
