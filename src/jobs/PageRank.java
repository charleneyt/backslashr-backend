package jobs;

import java.util.*;
import java.util.regex.*;

import flame.*;
import kvs.*;

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

        FlamePairRDD state = ctx.fromTable("crawl", r -> {
                                        if (r.columns().contains("url") && r.get("url").length() > 0 && r.columns().contains("page") && r.get("page").length() > 0){
                                            try {
                                                String url = r.get("url");
                                                String normalizedUrl = normalizeImpl(url, null, new StringBuilder());
                                                String foundUrl = findUrl(r.get("page"), normalizedUrl);
                                                return url + ",1.0,1.0," + foundUrl;
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                                return null;
                                            }
                                        } else {return null;}
                                    })
                                    // then split the String into FlamePair
                                    .mapToPair(s -> {
                                        if (s != null && s.length() > 0){
                                            int idx = s.indexOf(",");
                                            return new FlamePair(s.substring(0, idx), s.substring(idx + 1));
                                        } else {return null;}
                                    });
        int count = 0;
        state.saveAsTable("state-" + count);
        // Thread.sleep(FlameContext.getKVS().count("state-" + count)/10);

        while (true){
            count++;
            FlamePairRDD transfer = state.flatMapToPair(pair -> {
                                            List<FlamePair> ret = new ArrayList<FlamePair>();
                                            if (!"".equals(pair._1())){
                                                int firstComma = pair._2().indexOf(",");
                                                int secondComma = pair._2().indexOf(",", firstComma+1);
                                                if (firstComma == -1 || secondComma == -1){
                                                    return ret;
                                                }
                                                double currRank = Double.parseDouble(pair._2().substring(0, firstComma));
                                                if (secondComma != pair._2().length() - 1){
                                                    String urlList = pair._2().substring(secondComma+1);
                                                    String[] links = urlList.split(",");
                                                    int n = links.length;
                                                    if (n == 0){
                                                        return ret;
                                                    }
                                                    String val = String.valueOf(0.85 * currRank / n);
                                                    for (String link : links){
                                                        FlamePair generatedPair = new FlamePair(link, val);
                                                        ret.add(generatedPair);
                                                        // System.out.println(generatedPair);
                                                    }
                                                }
                                            }
                                            return ret;
                                        })
                                        .foldByKey("0.15", (d1, d2) -> String.valueOf(Double.parseDouble(d1) + Double.parseDouble(d2)));
            transfer.saveAsTable("transfer-" + count);
            // Thread.sleep(FlameContext.getKVS().count("transfer-" + count)/10);

            state = transfer.join(state)
                            .flatMapToPair(pair -> {
                                List<FlamePair> ret = new ArrayList<>();
                                if (!"".equals(pair._1())){
                                    String str = pair._2();
                                    int firstComma = str.indexOf(",");
                                    int secondComma = str.indexOf(",", firstComma+1);
                                    int thirdComma = str.indexOf(",", secondComma+1);
                                    if (firstComma == -1 || secondComma == -1 || thirdComma == -1){
                                        return ret;
                                    }
                                    String newVal = str.substring(0, secondComma) + str.substring(thirdComma);
                                    ret.add(new FlamePair(pair._1(), newVal));
                                }
                                return ret;
                            });
            state.saveAsTable("state-" + count);
            // Thread.sleep(FlameContext.getKVS().count("state-" + count)/10);
            
            FlameRDD diffTable = state.flatMap(pair -> {
                                List<String> ret = new ArrayList<>();
                                if (!"".equals(pair._1())){
                                    String str = pair._2();
                                    int firstComma = str.indexOf(",");
                                    int secondComma = str.indexOf(",", firstComma+1);
                                    if (firstComma == -1 || secondComma == -1){
                                        return ret;
                                    }
                                    double currRank = Double.parseDouble(pair._2().substring(0, firstComma));
                                    double prevRank = Double.parseDouble(pair._2().substring(firstComma+1, secondComma));
                                    ret.add(String.valueOf(Math.abs(currRank - prevRank)));
                                }
                                return ret;
                            });
            // EC 2 enhanced convergence: converge sooner if percentage is satisfied!
            boolean converged = false;
            if (inputThreshold){
                List<String> diffList = diffTable.collect();
                int needed = (int) (diffList.size() * thres / 100 + 0.5);
                for (String diff : diffList){
                    if (Double.parseDouble(diff) <= convergence){
                        needed--;
                    }
                    if (needed == 0){
                      System.out.println("Checked with enhanced convergence!");
                        converged = true;
                        break;
                    }
                }
            } else {
                String maxDiffStr = diffTable.fold("0.0", (s1, s2) -> {
                                double s1Double = Double.parseDouble(s1);
                                double s2Double = Double.parseDouble(s2);
                                if (s1Double < s2Double){
                                    return s2;
                                } else {
                                    return s1;
                                }
                            });
                if (Double.parseDouble(maxDiffStr) < convergence){
                  System.out.println("Checked with regular convergence!");
                    break;
                }
            }
            if (converged){
                break;
            }
        }
        System.out.println("Total iteration: " + count);
        state.saveAsTable("state");
        state = state.flatMapToPair(pair -> {
        				KVSClient kvs = new KVSClient("localhost:8000");
                        List<FlamePair> ret = new ArrayList<>();
                        if (!"".equals(pair._1())){
                            String str = pair._2();
                            int firstComma = str.indexOf(",");
                            int secondComma = str.indexOf(",", firstComma+1);
                            if (firstComma == -1 || secondComma == -1){
                                return ret;
                            }
                            String currRankStr = pair._2().substring(0, firstComma);
                            kvs.put("pageranks", pair._1(), "rank", currRankStr.getBytes());
                        }
                        return ret;
                    });
      ctx.output("OK");
    }

    static String findUrl(String s, String originalUrl) throws Exception {
        String urls = "";
        Matcher m = Pattern.compile("<[Aa][\\s\\S&&[^>]]*[Hh][Rr][Ee][Ff]=[\"\']?([\\S&&[^\"\'<>]]+)[\"\']?[\\s\\S&&[^>]]*>([\\s\\S&&[^<]]*)</[Aa]>")
                           .matcher(s);
        StringBuilder sb = new StringBuilder();
        Set<String> seen = new HashSet<>();
        while (m.find()){
          if (m.group(1) == null){
            continue;
          }
          String url = m.group(1);
          boolean unwantedType = false;
          int dot = url.lastIndexOf(".");
          if (dot != -1 && (dot == url.length()-4 || dot == url.length()-5)){
            switch (url.substring(dot).toLowerCase()){
              case ".jpg":
              case ".jpeg":
              case ".gif":
              case ".png":
              case ".txt":
                unwantedType = true;
                break;
            }
          }
          if (unwantedType){
            continue;
          }
          int sharpIdx = url.indexOf("#");
          if (sharpIdx != -1) {
            url = url.substring(0, sharpIdx);
            if (url.length() == 0) {
              continue;
            }
          }
          String copy = originalUrl;
          String normalizedUrl = normalizeImpl(url, copy, sb);
          if (!seen.contains(normalizedUrl)){
            seen.add(normalizedUrl);
            if (urls.length() > 0){
              urls += ",";
            }
            urls += normalizedUrl;
          }
        }
        return urls;
    }
    
    static String normalizeImpl(String url, String originalUrl, StringBuilder sb){
      sb.setLength(0);

      if (url == null || url.length() == 0){
        return null;
      }
      
      if (url.startsWith("http://")) {
        sb.append(url.substring(0, 7));
        url = url.substring(7);
        int col = url.indexOf(":");
        if (col != -1 && col+1 < url.length() && Character.isDigit(url.charAt(col+1))) {
          sb.append(url);
        } else {
          if (url.contains("/")) {
            int slash = url.indexOf("/");
            sb.append(url.substring(0, slash));
            sb.append(":80");
            sb.append(url.substring(slash));
          } else {
            sb.append(url + ":80");
          }
        }
      } else if (url.startsWith("https://")) {
        sb.append(url.substring(0, 7));
        url = url.substring(8);
        int col = url.indexOf(":");
        if (col != -1 && col+1 < url.length() && Character.isDigit(url.charAt(col+1))) {
          sb.append(url);
        } else {
          if (url.contains("/")) {
            int slash = url.indexOf("/");
            sb.append(url.substring(0, slash));
            sb.append(":443");
            sb.append(url.substring(slash));
          } else {
            sb.append(url + ":443");
          }
        }
      } else if (!url.contains("://")) {
        if (originalUrl.startsWith("http://")) {
          sb.append(originalUrl.substring(0, 7));
          originalUrl = originalUrl.substring(7);
        } else {
          sb.append(originalUrl.substring(0, 8));
          originalUrl = originalUrl.substring(8);
        }
        if (url.startsWith("/")) {
          sb.append(originalUrl.substring(0, originalUrl.indexOf("/")));
          sb.append(url);
        } else {
          int lastSlash = originalUrl.lastIndexOf("/");
          if (lastSlash == -1){
            return null;
          }
          originalUrl = originalUrl.substring(0, lastSlash);
  
          while (url.startsWith("../")){
            lastSlash = originalUrl.lastIndexOf("/");
            if (lastSlash == -1){
              return null;
            }
            if (url.length() == 3){
              url = url.substring(2);
            } else {
              url = url.substring(3);
            }
            originalUrl = originalUrl.substring(0, lastSlash);
          }
          if ("..".equals(url)){
            return null;
          }
  
          sb.append(originalUrl);
          if (url.startsWith("/")){
            sb.append(url);
          } else {
            sb.append("/" + url);
          }
        }
      }
      return sb.toString();
    }
}
