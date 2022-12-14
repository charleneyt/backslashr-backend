package jobs;

import flame.FlameContext;
import kvs.KVSClient;

public class CollectGarbage {
	public static void run(FlameContext ctx, String[] args) throws Exception {
		System.out.println("Executing garbage collector ...");

		String tableName = args[0];
		KVSClient kvs = FlameContext.getKVS();
		kvs.clean(tableName);
	}

}
