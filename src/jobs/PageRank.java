package jobs;

import java.util.*;

import flame.*;
import tools.Hasher;

public class PageRank {
	static boolean inputThreshold = false;

	public static void run(FlameContext ctx, String[] args) throws Exception {
		System.out.println("Executing pagerank ...");
		// add check for args, should be 1 for convergence threshold, and 1 optional
		if (args.length < 1 || args.length > 2) {
			ctx.output(
					"We are expecting one argument t for the convergence threshold, and one optional argument for percentage threshold!");
			return;
		}
		double convergence = Double.parseDouble(args[0]);

		// EC 2 enhanced convergence criterion
		double thres = -1.0;
		if (args.length == 2) {
			thres = Double.parseDouble(args[1]);
			if (thres > 0 && thres < 100) {
				inputThreshold = true;
			}
		}

		FlamePairRDD state = ctx.fromTable("outdegrees", r -> {
			if (r.columns().contains("links") && r.get("links").length() >= 0) {
				return r.key() + ",1.0,1.0," + r.get("links");
			} else {
				return null;
			}
		}).mapToPair(s -> {
//			System.out.println("S = " + s);
			if (s != null && s.length() > 0) {
				int idx = s.indexOf(",");
				return new FlamePair(s.substring(0, idx), s.substring(idx + 1));
			} else {
				return null;
			}
		});
		int count = 0;
		System.out.println("INITIAL STATE TABLE = " + state.getTableName() + "-" + count);

//		while (true) {
		count++;
		FlamePairRDD transfer = state.flatMapToPair(pair -> {
//				System.out.println("TRANSFER = " + pair._1() + " ===> " + pair._2());
			List<FlamePair> ret = new ArrayList<FlamePair>();
			if (!"".equals(pair._1())) {
				ret.add(new FlamePair(pair._1(), "0.0"));
				int firstComma = pair._2().indexOf(",");
				int secondComma = pair._2().indexOf(",", firstComma + 1);
				if (firstComma == -1 || secondComma == -1) {
					return ret;
				}
				double currRank = Double.parseDouble(pair._2().substring(0, firstComma));
				if (secondComma != pair._2().length() - 1) {
					String urlList = pair._2().substring(secondComma + 1);
					String[] links = urlList.split(",");
					int n = links.length;
					if (n == 0) {
						return ret;
					}
					String val = String.valueOf(0.85 * currRank / n);
					for (String link : links) {
						String hashLink = Hasher.hash(link);
						FlamePair generatedPair = new FlamePair(hashLink, val);
						ret.add(generatedPair);
					}
				}
			}
			return ret;
		});
		System.out.println("TRANSFER TABLE = " + transfer.getTableName() + "-" + count);
		transfer = transfer.foldByKey("0.15",
				(d1, d2) -> String.valueOf(Double.parseDouble(d1) + Double.parseDouble(d2)));

		System.out.println("TRANSFER AFTER FOLD TABLE = " + transfer.getTableName() + "-" + count);
//			System.out.println("STATE TABLE BEFORE JOIN  = " + state.getTableName() + "-" + count);

//			state = transfer.join(state);
//			System.out.println("STATE TABLE AFTER JOIN = " + state.getTableName() + "-" + count);
//
//			state.flatMapToPair(pair -> {
//				System.out.println("SHIFT = " + pair._1() + " ===> " + pair._2());
//				List<FlamePair> ret = new ArrayList<>();
//				if (!"".equals(pair._1())) {
//					String str = pair._2();
//					int firstComma = str.indexOf(",");
//					int secondComma = str.indexOf(",", firstComma + 1);
//					int thirdComma = str.indexOf(",", secondComma + 1);
//					if (firstComma == -1 || secondComma == -1 || thirdComma == -1) {
//						return ret;
//					}
//					String newVal = str.substring(0, secondComma) + str.substring(thirdComma);
//					ret.add(new FlamePair(pair._1(), newVal));
//				}
//				return ret;
//			});

		state = transfer.join(state).flatMapToPair(pair -> {
//				System.out.println("SHIFT = " + pair._1() + " ===> " + pair._2());
			List<FlamePair> ret = new ArrayList<>();
			if (!"".equals(pair._1())) {
				String str = pair._2();
				int firstComma = str.indexOf(",");
				int secondComma = str.indexOf(",", firstComma + 1);
				int thirdComma = str.indexOf(",", secondComma + 1);
				if (firstComma == -1 || secondComma == -1 || thirdComma == -1) {
					return ret;
				}
				String newVal = str.substring(0, secondComma) + str.substring(thirdComma);
				ret.add(new FlamePair(pair._1(), newVal));
			}
			return ret;
		});
		System.out.println("STATE TABLE AFTER SHIFTING = " + state.getTableName() + "-" + count);

		FlameRDD diffTable = state.flatMap(pair -> {
//				System.out.println("DIFF = " + pair._1() + " ===> " + pair._2());
			List<String> ret = new ArrayList<>();
			if (!"".equals(pair._1())) {
				String str = pair._2();
				int firstComma = str.indexOf(",");
				int secondComma = str.indexOf(",", firstComma + 1);
				if (firstComma == -1 || secondComma == -1) {
					return ret;
				}
				double currRank = Double.parseDouble(pair._2().substring(0, firstComma));
				double prevRank = Double.parseDouble(pair._2().substring(firstComma + 1, secondComma));
				ret.add(String.valueOf(Math.abs(currRank - prevRank)));
			}
			return ret;
		});
		System.out.println("DIFF TABLE = " + diffTable.getTableName() + "-" + count);

		// EC 2 enhanced convergence: converge sooner if percentage is satisfied!
		boolean converged = false;
//			if (inputThreshold) {
//				List<String> diffList = diffTable.collect();
//				int needed = (int) (diffList.size() * thres / 100 + 0.5);
//				for (String diff : diffList) {
//					if (Double.parseDouble(diff) <= convergence) {
//						needed--;
//					}
//					if (needed == 0) {
//						System.out.println("Checked with enhanced convergence!");
//						converged = true;
//						break;
//					}
//				}
//			} else {
		String maxDiffStr = diffTable.fold("0.0", (s1, s2) -> {
			double s1Double = Double.parseDouble(s1);
			double s2Double = Double.parseDouble(s2);
			if (s1Double < s2Double) {
				return s2;
			} else {
				return s1;
			}
		});
		System.out.println("Check max difference => " + maxDiffStr + " : " + convergence);
		if (Double.parseDouble(maxDiffStr) < convergence) {
			System.out.println("Checked with regular convergence!");
//				break;
		}
//			}
//			if (converged) {
//				break;
//			}
		System.out.println("Round " + count + ", not converged");
//		}
		System.out.println("Total iteration: " + count);

		state = state.flatMapToPair(pair -> {
			List<FlamePair> ret = new ArrayList<>();
			if (!"".equals(pair._1())) {
				String str = pair._2();
				int firstComma = str.indexOf(",");
				if (firstComma == -1) {
					return ret;
				}
				ret.add(new FlamePair(pair._1(), str.substring(0, firstComma)));
			}
			return ret;
		});
		state.saveAsTable("pageranks");
		ctx.output("OK");
	}
}
