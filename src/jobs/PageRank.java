package jobs;

import java.util.*;

import flame.*;
// import kvs.*;
import tools.*;

public class PageRank {
    static boolean inputThreshold = false;

    public static void run(FlameContext ctx, String[] args) throws Exception {
      // add check for args, should be 1 for convergence threshold, and 1 optional
        if (args.length < 1 || args.length > 2){
            ctx.output("We are expecting one argument t for the convergence threshold, and one optional argument for percentage threshold!");
            return;
        }
        double convergence = Double.parseDouble(args[0]);

        // EC 2 enhanced convergence criterion
        double thres = -1.0;
        if (args.length == 2){
            thres = Double.parseDouble(args[1]);
            if (thres > 0 && thres < 100){
                inputThreshold = true;
            }
        }
        
        // if (FlameContext.getKVS() == null){
        //     FlameContext.setKVS("localhost:8000");
        // }
        // KVSClient kvs = FlameContext.getKVS();

        FlamePairRDD state = ctx.fromTable("outdegrees", r -> {
                                        if (r.columns().contains("url") && r.get("url").length() > 0 && r.columns().contains("links") && r.get("links").length() > 0){
                                            try {
                                                return r.key() + "|" + r.get("url") + ",1.0,1.0," + r.get("links");
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                                return null;
                                            }
                                        } else {return null;}
                                    })
                                    // then split the String into FlamePair
                                    .mapToPair(s -> {
                                        if (s != null && s.length() > 0){
                                            int idx = s.indexOf("|");
                                            return new FlamePair(s.substring(0, idx), s.substring(idx + 1));
                                        } else {return null;}
                                    });
        int count = 0;
        // Thread.sleep(kvs.count(state.getTableName())/10);
        state.saveAsTable("state-" + count);

        while (true){
            count++;
            FlamePairRDD transfer = state.flatMapToPair(pair -> {
                                            List<FlamePair> ret = new ArrayList<FlamePair>();
                                            if (!"".equals(pair._1())){
                                                int firstComma = pair._2().indexOf(",");
                                                int secondComma = pair._2().indexOf(",", firstComma+1);
                                                int thirdComma = pair._2().indexOf(",", secondComma+1);
                                                if (firstComma == -1 || secondComma == -1 || thirdComma == -1){
                                                    return ret;
                                                }
                                                double currRank = Double.parseDouble(pair._2().substring(firstComma+1, secondComma));
                                                if (thirdComma != pair._2().length() - 1){
                                                    String urlList = pair._2().substring(thirdComma+1);
                                                    String[] links = urlList.split(",");
                                                    int n = links.length;
                                                    if (n == 0){
                                                        return ret;
                                                    }
                                                    String val = String.valueOf(0.85 * currRank / n);
                                                    for (String link : links){
                                                        String key = Hasher.hash(link);
                                                        if ("".equals(key)){
                                                            System.out.println("Hashed an empty link: "+link);
                                                        } else {
                                                            FlamePair generatedPair = new FlamePair(key, val);
                                                            ret.add(generatedPair);
                                                            // System.out.println(generatedPair);
                                                        }
                                                    }
                                                }
                                            }
                                            return ret;
                                        })
                                        .foldByKey("0.15", (d1, d2) -> String.valueOf(Double.parseDouble(d1) + Double.parseDouble(d2)));
            // Thread.sleep(kvs.count(transfer.getTableName())/10);
            transfer.saveAsTable("transfer-" + count);
//            Thread.sleep(kvs.count("transfer-" + count)/10);

            state = transfer.join(state)
                            .flatMapToPair(pair -> {
                                List<FlamePair> ret = new ArrayList<>();
                                if (!"".equals(pair._1())){
                                    String str = pair._2();
                                    int firstComma = str.indexOf(",");
                                    int secondComma = str.indexOf(",", firstComma+1);
                                    int thirdComma = str.indexOf(",", secondComma+1);
                                    int fourthComma = str.indexOf(",", thirdComma+1);
                                    if (firstComma == -1 || secondComma == -1 || thirdComma == -1 || fourthComma == -1){
                                        return ret;
                                    }
                                    String newVal = str.substring(firstComma+1, secondComma) +","+ str.substring(0, firstComma) + str.substring(secondComma, thirdComma) + str.substring(fourthComma);
                                    ret.add(new FlamePair(pair._1(), newVal));
                                }
                                return ret;
                            });
            // Thread.sleep(kvs.count(state.getTableName())/10);
            state.saveAsTable("state-" + count);
//            Thread.sleep(kvs.count("state-" + count)/10);
            
            FlameRDD diffTable = state.flatMap(pair -> {
                                List<String> ret = new ArrayList<>();
                                if (!"".equals(pair._1())){
                                    String str = pair._2();
                                    int firstComma = str.indexOf(",");
                                    int secondComma = str.indexOf(",", firstComma+1);
                                    int thirdComma = pair._2().indexOf(",", secondComma+1);
                                    if (firstComma == -1 || secondComma == -1 || thirdComma == -1){
                                        return ret;
                                    }
                                    double currRank = Double.parseDouble(pair._2().substring(firstComma+1, secondComma));
                                    double prevRank = Double.parseDouble(pair._2().substring(secondComma+1, thirdComma));
                                    ret.add(str.substring(0, firstComma+1) + String.valueOf(Math.abs(currRank - prevRank)));
                                }
                                return ret;
                            });
            diffTable.saveAsTable("diff-" + count);

            // EC 2 enhanced convergence: converge sooner if percentage is satisfied!
            boolean converged = false;
            if (inputThreshold){
                List<String> diffList = diffTable.collect();
                int needed = (int) (diffList.size() * thres / 100 + 0.5);
                for (String diff : diffList){
                	int firstComma = diff.indexOf(",");
                	if (firstComma != -1) {
                		if (Double.parseDouble(diff.substring(firstComma+1)) <= convergence){
	                      needed--;
	                  }
                	}
                  if (needed == 0){
                    System.out.println("Checked with enhanced convergence!");
                    converged = true;
                    break;
                  }
                }
            } else {
                String maxDiffStr = diffTable.fold("0.0", (s1, s2) -> {
                                double s1Double = Double.parseDouble(s1.substring(s1.indexOf(",")+1));
                                double s2Double = Double.parseDouble(s2.substring(s2.indexOf(",")+1));
                                if (s1Double < s2Double){
                                    return String.valueOf(s2Double);
                                } else {
                                    return String.valueOf(s1Double);
                                }
                            });
                System.out.println(maxDiffStr);
                if (Double.parseDouble(maxDiffStr) < convergence){
                    System.out.println("Checked with regular convergence!");
                    break;
                }
            }
            if (converged){
                break;
            }
            System.out.println("Checked " + count + " rounds, still not converged");
        }
        System.out.println("Total iteration: " + count);
        // state.saveAsTable("state");
        state = state.flatMapToPair(pair -> {
                List<FlamePair> ret = new ArrayList<>();
                if (!"".equals(pair._1())){
                    String str = pair._2();
                    int firstComma = str.indexOf(",");
                    int secondComma = str.indexOf(",", firstComma+1);
                    if (firstComma == -1 || secondComma == -1){
                        return ret;
                    }
                    ret.add(new FlamePair(pair._1(), str.substring(0, secondComma)));
                }
                return ret;
              });
    //    Thread.sleep(kvs.count(state.getTableName())/10);
       state.saveAsTable("pageranks");
       ctx.output("OK");
    }
}
