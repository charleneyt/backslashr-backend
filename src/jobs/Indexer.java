package jobs;

import flame.*;
import kvs.*;
import tools.*;

import java.util.*;
public class Indexer {
    public static void run(FlameContext ctx, String[] args) throws Exception {
    	System.out.println("Executing indexer ...");
        // use fromTable to convert each row to a string of url,page
        FlamePairRDD imm = ctx.fromTable("content", r -> {
                                        if (r.get("page") != null && r.get("page").length() > 0){
                                            return r.get("url") + "," + r.get("page");
                                        } else return "";
                                    })
                                    // then split the String into FlamePair
                                    .mapToPair(s -> {
                                        if (s.length() > 0){
                                            int idx = s.indexOf(",");
                                            return new FlamePair(s.substring(0, idx), s.substring(idx + 1));
                                        } else {return new FlamePair("", "");}
                                    });
                                    // use flatMapToPair to create Iterable<FlamePair>
        
        FlamePairRDD urlsTable = imm.flatMapToPair(pair -> {
                                        if (FlameContext.getKVS() == null){
                                            FlameContext.setKVS("localhost:8000");
                                        }
                                        KVSClient kvs = FlameContext.getKVS();
        								// KVSClient kvs = new KVSClient("localhost:8000");
                                        List<FlamePair> ret = new ArrayList<FlamePair>();
                                        if (!"".equals(pair._1())){
                                            String url = pair._1();
                                            String content = pair._2().replaceAll("[.,:;!\\?\'\"()-]", " ").toLowerCase();
                                            String[] words = content.split("\\s+");

                                            kvs.put("crawl", Hasher.hash(url), "wordCount", String.valueOf(words.length));

                                            // example entry: (word, index1 index2 index3)
                                            Map<String, String> original = new HashMap<>();
                                            Map<String, String> stem = new HashMap<>();
                                            for (int i = 0; i < words.length; i++){
                                                String word = words[i];
                                                if (word != null && word.length() > 0){
                                                    if (!original.containsKey(word)){
                                                        ret.add(new FlamePair(word, url));
                                                        original.put(word, String.valueOf(i+1));
                                                    } else {
                                                        original.put(word, original.get(word) + " " + (i+1));
                                                    }

                                                    // EC 3 add support for stemming
                                                    Stemmer stemmer = new Stemmer();
                                                    stemmer.add(word.toCharArray(), word.length());
                                                    stemmer.stem();
                                                    String stemWord = stemmer.toString();
                                                    if (stemWord != null && stemWord.length() > 0){
                                                        if (!stem.containsKey(stemWord)){
                                                            if (!stemWord.equals(word)){
                                                                ret.add(new FlamePair(stemWord, url));
                                                            }
                                                            stem.put(stemWord, String.valueOf(i+1));
                                                        } else {
                                                            stem.put(stemWord, stem.get(stemWord) + " " + (i+1));
                                                        }
                                                    }
                                                }
                                            }
                                            for (String word : stem.keySet()){
                                                // ec1: word, url, index1 index2 index3
                                                // 12/6 add a check to only add words appearing more than once
                                                if (stem.get(word).indexOf(" ") != -1){
                                                    kvs.put("inverted-hs", word, url, stem.get(word).getBytes());
                                                }
                                            }
                                            for (String word : original.keySet()){
                                                // 12/6 add a check to only add words appearing more than once
                                                if (!stem.containsKey(word) && original.get(word).indexOf(" ") != -1){
                                                    // ec1: word, url, index1 index2 index3
                                                    kvs.put("inverted-hs", word, url, original.get(word).getBytes());
                                                }
                                            }
                                        }
                                        // 12/6 don't return pairs!
                                        return null;
                                    });
                                    // .foldByKey("", (s1, s2) -> {
                                    //     if ("".equals(s1) && "".equals(s2)){
                                    //         return "";
                                    //     } else if ("".equals(s1)){
                                    //         return s2;
                                    //     } else if ("".equals(s2)){
                                    //         return s1;
                                    //     } else {
                                    //         return s1 + "," + s2;
                                    //     }
                                    // });
        // urlsTable.saveAsTable("index-regular");
        // urlsTable.saveAsTable("index");
        // Thread.sleep(FlameContext.getKVS().count("index-regular")/10);

        // EC 1 making a urls2 column that lists url by desc order of occurrences, and append those occurrence to url
        // FlamePairRDD urls2Table = ctx.fromTable("inverted-hs", row -> {
        //                                 // use treemap with count of indices as key, url1 url2 url3 as value
        //                                 TreeMap<Integer, String> currWordMap = new TreeMap<>(Collections.reverseOrder());

        //                                 for (String colName : row.columns()){
        //                                     int count = row.get(colName).split(" ").length;
        //                                     if (count == 0) continue;
        //                                     currWordMap.put(count, currWordMap.containsKey(count) ? currWordMap.get(count) + " " + colName : colName);
        //                                 }

        //                                 StringBuilder sb = new StringBuilder();
        //                                 for (String entry : currWordMap.values()){
        //                                     String[] splitStr = entry.split(" ");
        //                                     for (String link : splitStr){
        //                                         if (sb.length() > 0){
        //                                             sb.append(",");
        //                                         }
        //                                         sb.append(link + ":" + row.get(link));
        //                                     }
        //                                 }
        //                                 if (sb.length() > 0){
        //                                     return row.key() + "," + sb.toString();
        //                                 } else {
        //                                     return "";
        //                                 }
        //                             })
        //                             // then split the String into FlamePair
        //                             .mapToPair(s -> {
        //                                 if (s.length() > 0){
        //                                     int idx = s.indexOf(",");
        //                                     return new FlamePair(s.substring(0, idx), s.substring(idx + 1));
        //                                 } else {return new FlamePair("", "");}
        //                             })
        //                             .foldByKey("", (s1, s2) -> {
        //                                 if ("".equals(s1) && "".equals(s2)){
        //                                     return "";
        //                                 } else if ("".equals(s1)){
        //                                     return s2;
        //                                 } else if ("".equals(s2)){
        //                                     return s1;
        //                                 } else {
        //                                     return s1 + "," + s2;
        //                                 }
        //                             });
        // urls2Table.saveAsTable("index");
        // Thread.sleep(FlameContext.getKVS().count("index")/10);
        
        // FlamePairRDD urlsJoined = urlsTable.join(ec1);
        // urlsJoined.saveAsTable("joined");
        // Thread.sleep(4000);

        // FlamePairRDD output = urlsJoined.flatMapToPair(pair -> {
        //                                     List<FlamePair> ret = new ArrayList<FlamePair>();
        //                                     if (pair._2().contains(",")){
        //                                         String[] strPair = pair._2().split(",");
        //                                         ret.add(new FlamePair(pair._1(), strPair[0]));
        //                                         ret.add(new FlamePair(pair._1(), strPair[1]));
        //                                         return ret;
        //                                     } else {
        //                                         return ret;
        //                                     }
        //                                });
        // output.saveAsTable("result");

        // EC 3 add stemmedWord column to the index table for every index
        // KVSClient kvs = FlameContext.getKVS();
        // Iterator<Row> iter = kvs.scan("index", null, null);
        // String indexColName = null;
        // if (iter != null){
        //     while (iter.hasNext()){
        //         // row key is word, column is url, value is list of url (separated by comma)
        //         Row row = iter.next();
        //         if (row == null){
        //             break;
        //         }
        //         String word = row.key();
        //         if (word != null && word.length() > 0){
        //             // EC 3 add stemmed version to index
        //             Stemmer stemmer = new Stemmer();
        //             stemmer.add(word.toCharArray(), word.length());
        //             stemmer.stem();
        //             StringBuilder sb = new StringBuilder();
        //             // technically should only has one column
        //             for (String colName : row.columns()){
        //                 if (sb.length() > 0){
        //                     sb.append(",");
        //                 }
        //                 sb.append(row.get(colName));
        //                 indexColName = colName;
        //             }
        //             kvs.put("ec3", stemmer.toString(), row.key(), sb.toString().getBytes());
        //         }
        //     }
        // }

        // iter = kvs.scan("ec3", null, null);
        // if (iter != null){
        //     while (iter.hasNext()){
        //         // row key is stemmedWord, column is word, value is list of url (separated by comma)
        //         Row row = iter.next();
        //         if (row == null){
        //             break;
        //         }
        //         String stemmedWord = row.key();
        //         if (stemmedWord != null && stemmedWord.length() > 0){
        //             StringBuilder sb = new StringBuilder();
        //             for (String colName : row.columns()){
        //                 if (sb.length() > 0){
        //                     sb.append(",");
        //                 }
        //                 sb.append(row.get(colName));
        //             }
        //             Set<String> dedup = new HashSet<>(Arrays.asList(sb.toString().split(",")));
        //             String finalList = String.join(",", dedup);
        //             kvs.put("index", stemmedWord, "acc", finalList.getBytes());
        //         }
        //     }
        // }

        // EC 1 add urls2 column to the index table that adds :x, and sort by occurrences in desc order
        // KVSClient kvs = FlameContext.getKVS();
        // Iterator<Row> iter = kvs.scan("inverted-hs", null, null);
        // if (iter != null){
        //     while (iter.hasNext()){
        //         // row key is word, column is url, value is list of indices (separated by space)
        //         Row row = iter.next();
        //         if (row == null){
        //             break;
        //         }

        //         // use treemap with count of indices as key, url1 url2 url3 as value
        //         TreeMap<Integer, String> currWordMap = new TreeMap<>(Collections.reverseOrder());

        //         for (String colName : row.columns()){
        //             int count = row.get(colName).split(" ").length;
        //             if (count == 0) continue;
        //             currWordMap.put(count, currWordMap.containsKey(count) ? currWordMap.get(count) + " " + colName : colName);
        //         }

        //         StringBuilder sb = new StringBuilder();
        //         for (String entry : currWordMap.values()){
        //             String[] splitStr = entry.split(" ");
        //             for (String link : splitStr){
        //                 if (sb.length() > 0){
        //                     sb.append(",");
        //                 }
        //                 sb.append(link + ":" + row.get(link));
        //             }
        //         }
        //         if (sb.length() > 0){
        //             kvs.put("index", row.key(), "urls2", sb.toString().getBytes());
        //         }
        //     }
        // }
        ctx.output("OK");
    }
    
    public static class Stemmer
    {  private char[] b;
       private int i,     /* offset into b */
                   i_end, /* offset to end of stemmed word */
                   j, k;
       private static final int INC = 50;
                         /* unit of size whereby b is increased */
       public Stemmer()
       {  b = new char[INC];
          i = 0;
          i_end = 0;
       }

       /**
        * Add a character to the word being stemmed.  When you are finished
        * adding characters, you can call stem(void) to stem the word.
        */

       public void add(char ch)
       {  if (i == b.length)
          {  char[] new_b = new char[i+INC];
             for (int c = 0; c < i; c++) new_b[c] = b[c];
             b = new_b;
          }
          b[i++] = ch;
       }


       /** Adds wLen characters to the word being stemmed contained in a portion
        * of a char[] array. This is like repeated calls of add(char ch), but
        * faster.
        */

       public void add(char[] w, int wLen)
       {  if (i+wLen >= b.length)
          {  char[] new_b = new char[i+wLen+INC];
             for (int c = 0; c < i; c++) new_b[c] = b[c];
             b = new_b;
          }
          for (int c = 0; c < wLen; c++) b[i++] = w[c];
       }

       /**
        * After a word has been stemmed, it can be retrieved by toString(),
        * or a reference to the internal buffer can be retrieved by getResultBuffer
        * and getResultLength (which is generally more efficient.)
        */
       public String toString() { return new String(b,0,i_end); }

       /**
        * Returns the length of the word resulting from the stemming process.
        */
       public int getResultLength() { return i_end; }

       /**
        * Returns a reference to a character buffer containing the results of
        * the stemming process.  You also need to consult getResultLength()
        * to determine the length of the result.
        */
       public char[] getResultBuffer() { return b; }

       /* cons(i) is true <=> b[i] is a consonant. */

       private final boolean cons(int i)
       {  switch (b[i])
          {  case 'a': case 'e': case 'i': case 'o': case 'u': return false;
             case 'y': return (i==0) ? true : !cons(i-1);
             default: return true;
          }
       }

       /* m() measures the number of consonant sequences between 0 and j. if c is
          a consonant sequence and v a vowel sequence, and <..> indicates arbitrary
          presence,

             <c><v>       gives 0
             <c>vc<v>     gives 1
             <c>vcvc<v>   gives 2
             <c>vcvcvc<v> gives 3
             ....
       */

       private final int m()
       {  int n = 0;
          int i = 0;
          while(true)
          {  if (i > j) return n;
             if (! cons(i)) break; i++;
          }
          i++;
          while(true)
          {  while(true)
             {  if (i > j) return n;
                   if (cons(i)) break;
                   i++;
             }
             i++;
             n++;
             while(true)
             {  if (i > j) return n;
                if (! cons(i)) break;
                i++;
             }
             i++;
           }
       }

       /* vowelinstem() is true <=> 0,...j contains a vowel */

       private final boolean vowelinstem()
       {  int i; for (i = 0; i <= j; i++) if (! cons(i)) return true;
          return false;
       }

       /* doublec(j) is true <=> j,(j-1) contain a double consonant. */

       private final boolean doublec(int j)
       {  if (j < 1) return false;
          if (b[j] != b[j-1]) return false;
          return cons(j);
       }

       /* cvc(i) is true <=> i-2,i-1,i has the form consonant - vowel - consonant
          and also if the second c is not w,x or y. this is used when trying to
          restore an e at the end of a short word. e.g.

             cav(e), lov(e), hop(e), crim(e), but
             snow, box, tray.

       */

       private final boolean cvc(int i)
       {  if (i < 2 || !cons(i) || cons(i-1) || !cons(i-2)) return false;
          {  int ch = b[i];
             if (ch == 'w' || ch == 'x' || ch == 'y') return false;
          }
          return true;
       }

       private final boolean ends(String s)
       {  int l = s.length();
          int o = k-l+1;
          if (o < 0) return false;
          for (int i = 0; i < l; i++) if (b[o+i] != s.charAt(i)) return false;
          j = k-l;
          return true;
       }

       /* setto(s) sets (j+1),...k to the characters in the string s, readjusting
          k. */

       private final void setto(String s)
       {  int l = s.length();
          int o = j+1;
          for (int i = 0; i < l; i++) b[o+i] = s.charAt(i);
          k = j+l;
       }

       /* r(s) is used further down. */

       private final void r(String s) { if (m() > 0) setto(s); }

       /* step1() gets rid of plurals and -ed or -ing. e.g.

              caresses  ->  caress
              ponies    ->  poni
              ties      ->  ti
              caress    ->  caress
              cats      ->  cat

              feed      ->  feed
              agreed    ->  agree
              disabled  ->  disable

              matting   ->  mat
              mating    ->  mate
              meeting   ->  meet
              milling   ->  mill
              messing   ->  mess

              meetings  ->  meet

       */

       private final void step1()
       {  if (b[k] == 's')
          {  if (ends("sses")) k -= 2; else
             if (ends("ies")) setto("i"); else
             if (b[k-1] != 's') k--;
          }
          if (ends("eed")) { if (m() > 0) k--; } else
          if ((ends("ed") || ends("ing")) && vowelinstem())
          {  k = j;
             if (ends("at")) setto("ate"); else
             if (ends("bl")) setto("ble"); else
             if (ends("iz")) setto("ize"); else
             if (doublec(k))
             {  k--;
                {  int ch = b[k];
                   if (ch == 'l' || ch == 's' || ch == 'z') k++;
                }
             }
             else if (m() == 1 && cvc(k)) setto("e");
         }
       }

       /* step2() turns terminal y to i when there is another vowel in the stem. */

       private final void step2() { if (ends("y") && vowelinstem()) b[k] = 'i'; }

       /* step3() maps double suffices to single ones. so -ization ( = -ize plus
          -ation) maps to -ize etc. note that the string before the suffix must give
          m() > 0. */

       private final void step3() { if (k == 0) return; /* For Bug 1 */ switch (b[k-1])
       {
           case 'a': if (ends("ational")) { r("ate"); break; }
                     if (ends("tional")) { r("tion"); break; }
                     break;
           case 'c': if (ends("enci")) { r("ence"); break; }
                     if (ends("anci")) { r("ance"); break; }
                     break;
           case 'e': if (ends("izer")) { r("ize"); break; }
                     break;
           case 'l': if (ends("bli")) { r("ble"); break; }
                     if (ends("alli")) { r("al"); break; }
                     if (ends("entli")) { r("ent"); break; }
                     if (ends("eli")) { r("e"); break; }
                     if (ends("ousli")) { r("ous"); break; }
                     break;
           case 'o': if (ends("ization")) { r("ize"); break; }
                     if (ends("ation")) { r("ate"); break; }
                     if (ends("ator")) { r("ate"); break; }
                     break;
           case 's': if (ends("alism")) { r("al"); break; }
                     if (ends("iveness")) { r("ive"); break; }
                     if (ends("fulness")) { r("ful"); break; }
                     if (ends("ousness")) { r("ous"); break; }
                     break;
           case 't': if (ends("aliti")) { r("al"); break; }
                     if (ends("iviti")) { r("ive"); break; }
                     if (ends("biliti")) { r("ble"); break; }
                     break;
           case 'g': if (ends("logi")) { r("log"); break; }
       } }

       /* step4() deals with -ic-, -full, -ness etc. similar strategy to step3. */

       private final void step4() { switch (b[k])
       {
           case 'e': if (ends("icate")) { r("ic"); break; }
                     if (ends("ative")) { r(""); break; }
                     if (ends("alize")) { r("al"); break; }
                     break;
           case 'i': if (ends("iciti")) { r("ic"); break; }
                     break;
           case 'l': if (ends("ical")) { r("ic"); break; }
                     if (ends("ful")) { r(""); break; }
                     break;
           case 's': if (ends("ness")) { r(""); break; }
                     break;
       } }

       /* step5() takes off -ant, -ence etc., in context <c>vcvc<v>. */

       private final void step5()
       {   if (k == 0) return; /* for Bug 1 */ switch (b[k-1])
           {  case 'a': if (ends("al")) break; return;
              case 'c': if (ends("ance")) break;
                        if (ends("ence")) break; return;
              case 'e': if (ends("er")) break; return;
              case 'i': if (ends("ic")) break; return;
              case 'l': if (ends("able")) break;
                        if (ends("ible")) break; return;
              case 'n': if (ends("ant")) break;
                        if (ends("ement")) break;
                        if (ends("ment")) break;
                        /* element etc. not stripped before the m */
                        if (ends("ent")) break; return;
              case 'o': if (ends("ion") && j >= 0 && (b[j] == 's' || b[j] == 't')) break;
                                        /* j >= 0 fixes Bug 2 */
                        if (ends("ou")) break; return;
                        /* takes care of -ous */
              case 's': if (ends("ism")) break; return;
              case 't': if (ends("ate")) break;
                        if (ends("iti")) break; return;
              case 'u': if (ends("ous")) break; return;
              case 'v': if (ends("ive")) break; return;
              case 'z': if (ends("ize")) break; return;
              default: return;
           }
           if (m() > 1) k = j;
       }

       /* step6() removes a final -e if m() > 1. */

       private final void step6()
       {  j = k;
          if (b[k] == 'e')
          {  int a = m();
             if (a > 1 || a == 1 && !cvc(k-1)) k--;
          }
          if (b[k] == 'l' && doublec(k) && m() > 1) k--;
       }

       /** Stem the word placed into the Stemmer buffer through calls to add().
        * Returns true if the stemming process resulted in a word different
        * from the input.  You can retrieve the result with
        * getResultLength()/getResultBuffer() or toString().
        */
       public void stem()
       {  k = i - 1;
          if (k > 1) { step1(); step2(); step3(); step4(); step5(); step6(); }
          i_end = k+1; i = 0;
       }

       /** Test program for demonstrating the Stemmer.  It reads text from a
        * a list of files, stems each word, and writes the result to standard
        * output. Note that the word stemmed is expected to be in lower case:
        * forcing lower case must be done outside the Stemmer class.
        * Usage: Stemmer file-name file-name ...
        */
    }
}
