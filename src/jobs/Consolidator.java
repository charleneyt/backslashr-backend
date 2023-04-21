package jobs;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import flame.FlameContext;
import flame.FlameRDD;

public class Consolidator {
	public static void run(FlameContext ctx, String[] args) throws Exception {
		System.out.println("Executing consolidator ... at" + new Date());
		long startGetTime = System.currentTimeMillis();
		
		FlameRDD transform = ctx.consolidateFromTable(args[0], (r) -> {
			return r.get("value");
		});
		
		long endGetTime = System.currentTimeMillis();
		System.out.println("Finished consolidating the table! Took " + (endGetTime - startGetTime) + " ms.");
	}
		
}
