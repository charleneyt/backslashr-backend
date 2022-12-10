package jobs;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import flame.FlameContext;
import flame.FlameRDD;

public class Consolidator {
	public static void run(FlameContext ctx, String[] args) throws Exception {
		System.out.println("Executing consolidator ...updated as of 12/9 at " + new Date());
		long startGetTime = System.currentTimeMillis();
		
		HashMap<String, ArrayList<String>> consolidatedRows = new HashMap<>();
		
		FlameRDD transform = ctx.consolidateFromTable("index", (r) -> {
			return r.get("value");
		});
		
		long endGetTime = System.currentTimeMillis();
		System.out.println("Finished reading from content table! Took " + (endGetTime - startGetTime) + " ms.");
	}
		
}
