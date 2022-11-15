package webserver;

import java.net.URLDecoder;
import java.util.*;

public class RoutingTableEntry {
    String method;
    String pattern;
    String[] regexPattern;
    int splitLen;
    Route route;
    String host;

    public RoutingTableEntry(String method, String pattern, Route route){
        this(method, pattern, route, null);
    }

    public RoutingTableEntry(String method, String pattern, Route route, String host){
        this.method = method;
        this.pattern = pattern;
        this.route = route;

        regexPattern = pattern.split("/");
        splitLen = regexPattern.length;
        this.host = host;
    }

    public Map<String, String> comparePattern(String[] stringArray) throws Throwable{
        if (splitLen != stringArray.length){
            return null;
        }

        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < splitLen; i++){
            if (regexPattern[i].startsWith(":")){
                params.put(regexPattern[i].substring(1), URLDecoder.decode(stringArray[i], "UTF-8"));
            } else if (!regexPattern[i].equals(stringArray[i])){
                return null;
            }
        }
        return params;
    }
}
