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
		
		if (FlameContext.getKVS() == null) {
			FlameContext.setKVS("localhost:8000");
		}
		
		KVSClient kvs = FlameContext.getKVS();
		String tableName = args[0];
		String colName = args[1];
		
		List<String> values = new ArrayList<>();
		values.add("*twitter*");
		values.add("*youtube*");
		values.add("*instagram*");
		values.add("*facebook*");
		values.add("*amazon*");
		values.add("*cnnespanol*");
		values.add("*ads*");
		values.add("*espncricinfo*");
		values.add("*disneytermsofuse.com*");
		values.add("*marca.com*");
		values.add("*cadenaser.com*");
		values.add("*uesyndication.com*");
		values.add("*calciomercato.com*");
		values.add("*tuttosport.com*");
		values.add("*cadenaser.com*");
		values.add("*viacomcbsprivacy.com*");
		values.add("*as.com*");
		values.add("*retinatendencias.com*");
		values.add("*prisa.com*");
		values.add("*/stats/*");
		values.add("*/preview/*");
		values.add("*/calendar/*");
		values.add("*/week/*");
		values.add("*olympics.com*");
		values.add("*browsehappy.com*");
		values.add("*forum*");
		values.add("*apple.com*");
		values.add("*wpvip.com*");
		values.add("*snapchat.com*");
		values.add("*bank*");
		values.add("*map*");
		values.add("*video*");
		values.add("*waze*");
		values.add("*spotify*");
		values.add("*ticket*");
		values.add("*store*");
		values.add("*linkedin*");
		values.add("*teamworkonline.com*");
		values.add("*jamsadr.com*");
		values.add("*wsop.com*");
		values.add("*disqus*");
		values.add("*tigweb.org*");
		values.add("*health*");
		values.add("*gobrockport.com*");
		values.add("*pr.nba.com*");
		values.add("*picks.cbssports.com*");
		values.add("*footmercato.net*");
		values.add("*mundodeportivo.com*");
		values.add("*andro4all.com*");
		values.add("*nbcuni*");
		values.add("*meredith.com*");
		values.add("*shop*");
		values.add("*comcast*");
		values.add("*nasdaq.com*");
		values.add("*childrensplace.com*");
		values.add("*signup*");
		values.add("*optout*");
		values.add("*login*");
		values.add("*logout*");
		values.add("*sports.com*");
		values.add("*/author/*");
		values.add("*iberdrola.com*");
		
		for (String value : values) {
			kvs.put(tableName, Hasher.hash(value), colName, value.getBytes());
		}
		
		System.out.println("Saved table " + tableName);
	}
}
