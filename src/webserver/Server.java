/**
 * Server class that keeps listening to the port and use ThreadPoolManager to manage any accepted socket
 */
package webserver;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.security.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import javax.net.ssl.*;

import tools.SNIInspector;
import exceptions.*;

/**
 * @author Charlene Tam
 *
 */
public class Server implements Runnable {
	static Server HTTP_SERVER = null;
	static boolean HAS_HTTP_THREAD = false;
    static Server HTTPS_SERVER = null;
	static boolean HAS_HTTPS_THREAD = false;
    boolean instanceSecureType;
    ServerSocket ssock;
	final static int DEFAULT_PORT = 80;
    static Map<String, SessionImpl> sessions = new HashMap<>();
    public static Map<String, HostRecord> hostTable = new HashMap<>();
	
	public static int NUM_WORKERS = 20;
    static String CRLF = "\r\n";
    static String BOUNDARY = "A_FANCY_SEPARATOR_MADE_BY_CHARLENE_TAM";
    static String DELIMITER = "--";
    static String BOUNDARY_DELIMITER = CRLF + DELIMITER + BOUNDARY;
    static int TIMER_INTERVAL = 300000;

    static final String DEFAULT_JKS = "keystore.jks";
    static final String DEFAULT_PWD = "secret";

	public int port = DEFAULT_PORT;
    public int securePort = -1;
	public String directory = null;
    public List<RoutingTableEntry> routingTable = new LinkedList<>();
    
    public String host = null;
    public Route before = null;
    public Route after = null;

	private BlockingQueue<Socket> queue;
	private Worker[] workers;
	
	private Timer timer;

	public void run() {		
		queue = new LinkedBlockingDeque<>(NUM_WORKERS*10);
		workers = new Worker[NUM_WORKERS];
        Server server;
        if (instanceSecureType){
            server = HTTPS_SERVER;
        } else {
            server = HTTP_SERVER;
        }

		for (int i = 0; i < NUM_WORKERS; i++) {
			workers[i] = new Worker(server);
			new Thread(workers[i]).start();
		}
		
		timer = new Timer(TIMER_INTERVAL);
		new Thread(timer).start();
		
		try {
            // create new serversocket
            if (instanceSecureType){
                // HTTPS socket
                //  ssock = getServerSocket("src/cis5550/keystore.jks", "secret", securePort);
                // EC implementation: use regular serversocket, then get SNI Inspector to connect
//                // System.out.println(securePort);
                ssock = new ServerSocket(securePort);
            } else {
                ssock = new ServerSocket(port);
            }

			// always taking sock from queue and process
			while (true){
				Socket sock = ssock.accept();
				queue.put(sock); 
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    private SSLContext getSSLContext(String jks, String pwd) throws Exception{
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(jks), pwd.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, pwd.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        return sslContext;
    }

    private ServerSocket getServerSocket(String jks, String pwd, int usePort) throws Exception {
        return getSSLContext(jks, pwd).getServerSocketFactory().createServerSocket(usePort);
    }

    public static void get(String s, Route r) {
        createServer();
        startServer();
        HTTP_SERVER.routingTable.add(new RoutingTableEntry("GET", s, r, HTTP_SERVER.host));
        HTTPS_SERVER.routingTable.add(new RoutingTableEntry("GET", s, r, HTTPS_SERVER.host));
    }
    
    public static void post(String s, Route r) {
        createServer();
        startServer();
        HTTP_SERVER.routingTable.add(new RoutingTableEntry("POST", s, r, HTTP_SERVER.host));
        HTTPS_SERVER.routingTable.add(new RoutingTableEntry("POST", s, r, HTTPS_SERVER.host));
    }
    
    public static void put(String s, Route r) {
        createServer();
        startServer();
        HTTP_SERVER.routingTable.add(new RoutingTableEntry("PUT", s, r, HTTP_SERVER.host));
        HTTPS_SERVER.routingTable.add(new RoutingTableEntry("PUT", s, r, HTTPS_SERVER.host));
    }
    
    public static void before(Route r) {
        createServer();
        startServer();
        HTTP_SERVER.before = r;
        HTTPS_SERVER.before = r;
    }
    
    public static void after(Route r) {
        createServer();
        startServer();
        HTTP_SERVER.after = r;
        HTTPS_SERVER.after = r;
    }
    
    public static void port(int portToUse) {
        createServer();
        HTTP_SERVER.port = portToUse;
    }

    public static void securePort(int portToUse) {
        createServer();
        HTTPS_SERVER.securePort = portToUse;
    }

    public static void host(String h, String jksFile, String password){
        createServer();
        HTTP_SERVER.host = h;
        HTTPS_SERVER.host = h;
        hostTable.put(h, new HostRecord(h, jksFile, password));
    }
    
    private static void createServer() {
        if (HTTP_SERVER == null) {
            HTTP_SERVER = new Server();
            HTTP_SERVER.instanceSecureType = false;
        }
        if (HTTPS_SERVER == null) {
            HTTPS_SERVER = new Server();
            HTTPS_SERVER.instanceSecureType = true;
        }
    }
    
    private static void startServer() {
        if (!HAS_HTTP_THREAD) {
            new Thread(HTTP_SERVER).start();
            HAS_HTTP_THREAD = true;
        }
        if (!HAS_HTTPS_THREAD && HTTPS_SERVER.securePort != -1) {
            new Thread(HTTPS_SERVER).start();
            HAS_HTTPS_THREAD = true;
        }
    }
    
    public static class staticFiles{
        public static void location(String s) {
            createServer();
            if (!s.endsWith("/")){
                s += "/";
            }
            HTTP_SERVER.directory = s;
            HTTPS_SERVER.directory = s;
        }
    }

    public class Timer implements Runnable{
        // time to sleep in milliseconds
    	int timeIntervals;

        public Timer(int timeIntervals){
            this.timeIntervals = timeIntervals;
        }

        public void run(){
            while (true){
                for (Map.Entry<String, SessionImpl> entry : sessions.entrySet()){
                    if (!entry.getValue().valid){
                        sessions.remove(entry.getKey());
                    } else {
                        entry.getValue().invalidate();
                        if (!entry.getValue().valid){
                            sessions.remove(entry.getKey());
                        }
                    }
                }
                try {
					Thread.sleep(timeIntervals);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
            }
        }
    }
    
	public class Worker implements Runnable {
        Server serverInstance;
		Socket sock;
		BufferedInputStream br;
        PrintWriter pw;
        BufferedOutputStream bw;
        boolean addlStream = false;
        ByteArrayInputStream pre_br = null;

        Map<String, String> headers;
        int statusCode;
        String reasonPhrase;
        String method;
        String url;
        String protocol;
        String server;

        String filePath;
        String requestString;
        String contentType;
        int messageSize;
        byte[] messageBody;

        SessionImpl currSession = null;
        public boolean newSession = false;

        Map<String, String> params = null;
        Map<String, String> qparams = null;

        RequestImpl request;
        ResponseImpl response;
        RoutingTableEntry r;
        
        public Worker(Server instance) {
        	serverInstance = instance;
        }

        public void run() {
            try {
                // always taking sock from queue and process
                while (true){
                	sock = queue.take();
                    if (instanceSecureType){
                        addlStream = true;
                        boolean parse = false;
                        // for HTTPS socket, use SNIInspector to see if host has cert to establish secure connection
                        SNIInspector inspector = new SNIInspector();
                        try{
                            inspector.parseConnection(sock);
                            
                            pre_br = inspector.getInputStream();
                            parse = true;
                            SNIHostName hostName = inspector.getHostName();
                            if (hostName != null && hostTable.containsKey(hostName.getAsciiName())){
                                try{
                                    HostRecord record = hostTable.get(hostName.getAsciiName());
                                    if (record.jksFile != null && record.password != null){
                                        sock = getSSLContext(record.jksFile, record.password).getSocketFactory().createSocket(sock, inspector.getInputStream(), true);
                                        addlStream = false;
                                    } else {
                                        // covering host('foo.com', null, null) call
                                        sock = getSSLContext(DEFAULT_JKS, DEFAULT_PWD).getSocketFactory().createSocket(sock, inspector.getInputStream(), true);
                                        addlStream = false;
                                    }
                                } catch (Exception e){
                                    // anytime the validation fails, fall back to default cert and password
                                    sock = getSSLContext(DEFAULT_JKS, DEFAULT_PWD).getSocketFactory().createSocket(sock, inspector.getInputStream(), true);
                                    addlStream = false;
                                    e.printStackTrace();
                                }
                            }
                            else {
                                // fall back to default cert and password
                                sock = getSSLContext(DEFAULT_JKS, DEFAULT_PWD).getSocketFactory().createSocket(sock, inspector.getInputStream(), true);
                                addlStream = false;
                            }
                        } catch (Exception e){
                            // shouldn't really fail if request are made for HTTPS
                            e.printStackTrace();
                            if (!parse){
                                pre_br = new ByteArrayInputStream(inspector.buffer, 0, inspector.recordLength);
                            }
                        }
                        
                     }
                    listenAndResponse();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /** listens and generating response message to client, closes when client has sent over EOF
         * 
         * @throws Exception
         */
        private void listenAndResponse() throws Exception{
             if (addlStream && pre_br != null){
                 br = new BufferedInputStream(new SequenceInputStream(pre_br, sock.getInputStream()));
             } else {
                 br = new BufferedInputStream(sock.getInputStream());
             }
            // br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            pw = new PrintWriter(sock.getOutputStream());
            bw = new BufferedOutputStream(sock.getOutputStream());

            // invoke helper method to read and process message until EOF received
            try{executeUntilShutDown();}
            catch (Exception e) {}
            
            // close br and pw when socket received EOF (client called shutdownOutput())
            br.close();
            pw.close();
            bw.close();

            // close the socket when done
            sock.close();
        }

        /** helper method that keeps reading until meeting EOF
         *  when getting double CRLF, calls processRequest() to parse the info, and sends out response
         * 
         * @throws Exception
         */
        private void executeUntilShutDown() throws Exception {
            server = InetAddress.getLocalHost().getHostName();
            // c to read in char as int, then cast to char
            int c;
            char ch;
            
            // StringBuilder for new message,
            // booleans to remember status (only proceed to after line 123 if there are double CRLF)
            StringBuilder sb = new StringBuilder();
            boolean hasCR = false;
            boolean hasCRLF = false;

            r = null;

            statusCode = 200;
            reasonPhrase = "200 OK";

            // If the header is not present, keep reading until the client closes its half of the TCP connection
            while ((c = br.read()) != -1){
                ch = (char) c;
                sb.append(ch);
                
                // substitutes all newline char to space to ease the split later
                if (ch == '\r'){
                    hasCR = true;
                    continue;
                }
                else if (hasCR && ch == '\n'){
                    if (!hasCRLF){
                        hasCRLF = true;
                        continue;
                    }
                }
                else {
                    hasCR = false;
                    hasCRLF = false;
                    continue;
                }
                // ONLY REACH HERE if double CRLF is met!

                // start process request when received double CRLF
//                requestString = sb.toString();
//                FileWriter fw = new FileWriter("./server_log", true);
//                fw.write("request string is: " + requestString);
//                fw.flush();
                //  System.out.print("request string is: " + requestString);
                
                parseRequest();
                
                if (r != null){
                    useRouteAndSendResponse();
                    if (sock.isClosed()) break; // quit the read loop is connection is closed!
                } else {
                    sendStaticResponse();
                }

                // System.out.println("\n");

                // reset the sb and flags and restart the reading (from same socket)
                sb = new StringBuilder();
                hasCR = false;
                hasCRLF = false;
                
                r = null;

                statusCode = 200;
                reasonPhrase = "200 OK";

                currSession = null;
                newSession = false;
            }
        }

        /** process the request message by parsing and analyzing response info to be sent
         * 
         * @throws IOException
         */
        private void parseRequest() throws IOException {
            
            headers = new HashMap<>();
            
            int startIdx = -1;
            if (requestString.contains("PUT")) {
            	startIdx = requestString.indexOf("PUT");
            }
            else if (requestString.contains("GET")) {
            	startIdx = requestString.indexOf("GET");
            }
            else if (requestString.contains("POST")) {
            	startIdx = requestString.indexOf("POST");
            }
            else {
            	System.out.println("NOT a valid reqeust");
            	return;
            }
            
            requestString = requestString.substring(startIdx);

            // parse the request
            String[] headerInLines = requestString.split(CRLF);
            String[] headerParts = headerInLines[0].split(" "); // method, URL, protocol

            // Completeness check
            // 400 Bad Request if the method, URL, protocol header are missing from the request;
            if (headerInLines.length <= 1 || headerParts.length != 3){
            	System.out.println("400 triggered, v1");
//            	System.out.println("request string is: " + requestString);
                statusCode = 400;
                reasonPhrase = "400 Bad Request";
                return;
            }

            method = headerParts[0];
            url = headerParts[1];
            protocol = headerParts[2];
            // examine if there's Host, Content-Length, If-Modified-Since, Range
            for (int i = 1; i < headerInLines.length; i++){
                String[] headerFirstLine = headerInLines[i].split(": ");
                if (headerFirstLine.length == 2){
                    headers.put(headerFirstLine[0].toLowerCase(), headerFirstLine[1]);
                }
            }
            
            // 400 Bad Request if the Host: header are missing from the request;
            if (!headers.containsKey("host")){
            	System.out.println("400 triggered, no host");
                statusCode = 400;
                reasonPhrase = "400 Bad Request";
                return;
            }

            // if Content-Length exists, read extra char as message body
            int contentLength;
            if (headers.containsKey("content-length") && (contentLength = Integer.valueOf(headers.get("content-length").trim())) > 0){
                messageBody = new byte[contentLength];
                br.read(messageBody, 0, contentLength);
            } else {
                messageBody = new byte[0];
            }

            // protocol check
            // 505 HTTP Version Not Supported if the protocol is anything other than HTTP/1.1
            if (!"HTTP/1.1".equals(headerParts[2])){
                statusCode = 505;
                reasonPhrase = "505 HTTP Version Not Supported";
                return;
            }

            // method check
            switch (method){
                // 405 Not Allowed if the method is POST or PUT
                // check if we have matching route
                case "POST":
                case "PUT":
                    if (matchRoute(method, url)){
                        break;
                    }
                    statusCode = 405;
                    reasonPhrase = "405 Not Allowed";
                    return;
                case "GET":
                case "HEAD":
                    matchRoute(method, url);
                    break;
                // 501 Not Implemented if the method is something other than GET, HEAD, POST, or PUT
                    default:
                    statusCode = 501;
                    reasonPhrase = "501 Not Implemented";
                    return;
            }

            // check if there's cookie header with with the name SessionID and the value of that cookie is currently associated with a Session object
            if (headers.containsKey("cookie")){
                String cookie = headers.get("cookie");
                if (cookie.contains("SessionID=")){
                    String sessionId = cookie.substring(cookie.indexOf("SessionID=")+10).split(";")[0];
                    if (sessions.containsKey(sessionId)){
                        // check the session to prevent overshoot
                        SessionImpl tempSession = sessions.get(sessionId);
                        tempSession.invalidate();
                        if (tempSession.valid){
                            // only update last accessed time if it's still valid
                            tempSession.setLastAccessedTime(System.currentTimeMillis());
                            sessions.put(sessionId, tempSession);
                            currSession = tempSession;
                        } else {
                            currSession = null;
                        }
                    }
                }
            }

            if (r != null){
                parseQueryParams();
            } else {
                // Static file checks start here
                // if location() hasn't been called, return 404
                if (directory == null){
                    statusCode = 404;
                    reasonPhrase = "404 Not Found";
                    return;
                }

                // 403 Forbidden if the requested file exists but is not readable;
                // 404 Not Found if the requested file does not exist;
                filePath = directory + headerParts[1];
                if (url.contains("?")){
                    filePath = directory + url.split("\\?", 2)[0];
                } else {
                    filePath = directory + url;
                }

                // URL check: "requested URL" just from the request
                if (url.contains("..")){
                    // 403 Forbidden if URL contains ".."
                    statusCode = 403;
                    reasonPhrase = "403 Forbidden";
                    return;
                }

                Path file = Paths.get(filePath);
                if (!Files.exists(file)){
                    statusCode = 404;
                    reasonPhrase = "404 Not Found";
                    return;
                } else if (Files.isDirectory(file) || !Files.isReadable(file)){
                    statusCode = 403;
                    reasonPhrase = "403 Forbidden";
                    return;
                }

                // If the request would normally result in anything other than a 200 (OK) status, 
                // or if the passed If-Modified-Since date is invalid (date later than current time), 
                // or if the variant has been modified since the If-Modified-Since date
                // the response is exactly the same as for a normal GET.

                // If the variant has not been modified since a valid If-Modified-Since date, the server SHOULD return a 304 (Not Modified) response.
                if (headers.containsKey("if-modified-since")){
                    try {
                        ZonedDateTime lastModifiedSince = ZonedDateTime.parse(headers.get("if-modified-since"), DateTimeFormatter.RFC_1123_DATE_TIME);
                        FileTime fileTime = Files.readAttributes(file, BasicFileAttributes.class).lastModifiedTime();
                        ZonedDateTime fileLastModified = fileTime.toInstant().atZone(ZoneId.systemDefault());
                        if (!lastModifiedSince.isAfter(ZonedDateTime.now()) && lastModifiedSince.isAfter(fileLastModified)){
                            // Q: should the header contain Last-Modified: ?
                            // A: no need, https://edstem.org/us/courses/24571/discussion/1739948?comment=3973780
                            statusCode = 304;
                            reasonPhrase = "304 Not Modified";
                            return;
                        }
                    } catch (Exception e){}
                }

                // save Content-Type string based on file extension
                if (filePath.contains(".") && !filePath.endsWith(".")){
                    switch (filePath.substring(filePath.lastIndexOf('.') + 1).toUpperCase()){
                        case "JPG":
                        case "JPEG":
                            contentType = "image/jpeg";
                            break;
                        case "TXT":
                            contentType = "text/plain";
                            break;
                        case "HTML":
                            contentType = "text/html";
                            break;
                        default:
                            contentType = "application/octet-stream";
                            break;
                    }
                } else {
                    contentType = "application/octet-stream";
                }
            }
        }

        /** helper method for dynamic processing to parse query parameters from request
         * directly updates the qparams map in the instance
         */
        private void parseQueryParams(){
            if (url.contains("?")){
                if (qparams == null){
                    qparams = new HashMap<>();
                }
                parseQueryParams_Impl(url.split("\\?", 2)[1].split("&"));
            }
            if ("application/x-www-form-urlencoded".equals(headers.getOrDefault("content-type",null))){
                if (qparams == null){
                    qparams = new HashMap<>();
                }
                parseQueryParams_Impl(new String(messageBody).split("&"));
            }
        }

        /**
         * helper method to parse qparams (implementation for input of string array)
         * @param input
         */
        private void parseQueryParams_Impl(String[] input){
            for (String pair : input){
                if (pair.contains("=")){
                    try {
                        String[] pairSplit = pair.split("=", 2);
                        String name = URLDecoder.decode(pairSplit[0], "UTF-8");
                        String value = URLDecoder.decode(pairSplit[1], "UTF-8");
                        // Ed post #180
                        // if (qparams.containsKey(name)){
                        //     qparams.put(name, qparams.get(name) + ", " + value);
                        // } else {
                            qparams.put(name, value);
                        // }
                    }
                    catch (Exception e){
                        // System.out.println("Failed to parse query parameter, invalid input!");
                        e.printStackTrace();
                    }
                }
            }
        }

        /** helper method for dynamic routing processing, save the route if there's a match
         * 
         * @param method
         * @param path
         * @return true if there's an existing routing table entry matches with method and path
         */
        private boolean matchRoute(String method, String path){
            if (path.contains("?")){
                path = path.split("\\?", 2)[0];
            }
            String[] pathToArray = path.split("/");
            for (RoutingTableEntry entry : routingTable){
                if (entry.method.equals(method) 
                && (entry.host == null || entry.host.equals(headers.get("host")))){
                    try {
						params = entry.comparePattern(pathToArray);
					} catch (Throwable e) {
						continue;
					}
                    if (params != null){
                        r = entry;
                        return true;
                    }
                }
            }
            return false;
        }

        /** helper method for dynamic routing sending response
         * instantiate Request and Response objects, and pass them to the handle method on the Route.  
         * If this throws an exception, return the 500 response as required.
         * If not, set the Content-Length header and then write out the headers; then write out the body, if there is any
         */
        private void useRouteAndSendResponse() throws IOException{
            // parse query params from char array messageBody
            request = new RequestImpl(method, url, protocol, headers, qparams, params, (InetSocketAddress) sock.getRemoteSocketAddress(), messageBody, serverInstance);
            response = new ResponseImpl(method, url, protocol, serverInstance);
            request.worker(this);
            response.worker(this);
            if (currSession != null && currSession.valid){
                request.setSession(currSession);
            }
            

            try {
                response.header("Server", server);
                response.status(200, "OK");
                
                if (before != null){
                    before.handle(request, response);
                }
                
                response.before = false;
                Object result = r.route.handle(request, response);

                if (after != null){
                    after.handle(request, response);
                }
                
                // go straight to write anytime redirect is called
                if (!response.write){
                    if (result != null){
                        response.body(result.toString());
                        response.header("Content-Length", String.valueOf(response.bodyRaw.length));
                    } else if (response.bodyRaw != null && response.bodyRaw.length != 0){
                        // use bodyRaw and send bytes
                        response.header("Content-Length", String.valueOf(response.bodyRaw.length));
                    } else {
                        // not sending body
                        response.header("Content-Length", "0");
                    }
                    
                    sendDynamicResponse(false);
                } else {
                    // if write() is called during handling, close the connection
                	// System.out.println("stopping connection!");
                	sock.close();
                }
                
            }
            catch (HaltException e){
                // if halt() is called in before stage, immediately stops the request and send message
                // System.out.println("halt() is called, stop the request!");
            	if (!response.write){
                    response.status(e.statusCode, e.reasonPhrase);
                    response.body(e.reasonPhrase);
                    response.header("Content-Length", String.valueOf(response.bodyRaw.length));
                    sendDynamicResponse(false);
                }
            }
            catch (RedirectException e){
                // anytime redirect is called, immediately send the redirect response
                // status code, reasonPhrase and body updated in response.redirect()
                response.header("Content-Length", String.valueOf(response.bodyRaw.length));
                sendDynamicResponse(false);
            }
            catch (Exception e){
                // any other error, return 500, or close connection if write has been called
                 e.printStackTrace();
                if (!response.write){
                    response.status(500, "Internal Server Error");
                    response.body(response.reasonPhrase);
                    response.header("Content-Length", String.valueOf(response.bodyRaw.length));
                    sendDynamicResponse(false);
                } else {
                	// System.out.println("stopping connection!");
                	sock.close();
                }
            }
        }

        /**
         * helper method that sets type header if non exist, sends response header, and writes body if method is not HEAD, and there is content
         * 
         * @param calledFromWrite true means only need to write the headers
         * @throws IOException
         */
        public void sendDynamicResponse(boolean calledFromWrite) throws IOException{
            if (!response.headers.containsKey("Content-Type")){
                // response.type("text/plain");
                if (response.statusCode != 200){
                    response.type("text/plain");
                } else {
                    response.type("application/octet-stream");
                }
            }
            
            String messageHead = response.protocol + " " + response.statusCode + " " + response.reasonPhrase + CRLF;
            // System.out.print(messageHead);
            pw.write(messageHead);
            for (Map.Entry<String, Set<String>> entry: response.headers.entrySet()){
                String key = entry.getKey();
                // omit the content-length if using write
                if (calledFromWrite && key.equals("Content-Length")){
                    continue;
                }
                for (String value : entry.getValue()){
                    // System.out.print(key + ": " + value + CRLF);
                    pw.write(key + ": " + value + CRLF);
                }
            }
            // System.out.print(CRLF);
            pw.write(CRLF);
            pw.flush();

            // write() would directly write, skip here
            if (!calledFromWrite){
                // only send messageBody if content-length is greater than 0, and method is not HEAD
                if (!"HEAD".equals(response.method) && response.bodyRaw.length != 0){
                    bw.write(response.bodyRaw);
                    // System.out.print(new String(response.bodyRaw));
                    bw.flush();
                }
            }
        }
        
        
        /** helper method that sends response for staticFiles request
         * 
         * @throws Exception
         */
        private void sendStaticResponse() throws Exception{
            String serverName = InetAddress.getLocalHost().getHostName();
            
            // make return response
            if (statusCode != 200){

                // No message body sent for Error 304? per https://datatracker.ietf.org/doc/html/rfc2616#section-14.25
                // or do it like all other error conditions per instruction
                String message = "HTTP/1.1 " + reasonPhrase + CRLF
                                + "Server: " + serverName + CRLF
                                + "Content-Type: text/plain" + CRLF
                                + "Content-Length: " + reasonPhrase.length() + CRLF + CRLF;
                // System.out.print(message);
                pw.write(message);
                
                // only print body if method is GET, and error code is not 304  
                // (a 304 (not modified) response will be returned without any message-body.) per RFC 2616 14.25
                if (!"HEAD".equals(method) && statusCode != 304) {
                	// System.out.print(reasonPhrase);
                    pw.write(reasonPhrase);
                }
                
                pw.flush();
                

            } else {
                // Use Stream to open the file, remember to flush pw before sending messageBody!!
                FileInputStream fileStream = new FileInputStream(filePath);
                int contentLength = fileStream.available();
                
                // need to check range if method is GET, Content-Length will change, 
                // also 206 Partial Content should be returned as status, Content-Range needs to be sent in header (bytes 42-1233/1234)
                // 416 (Range Not Satisfiable) will be sent with (bytes */47022)
                if (headers.containsKey("range") && "GET".equals(method)){
                    List<Integer[]> ranges = getRangesList(headers.get("range"), contentLength);
                    if (ranges == null || ranges.size() == 0){
                        // go with 416, write full file in body with Content-Range "bytes */contentLength"
                        String message = protocol + " 416 Range Not Satisfiable" + CRLF
                                + "Server: " + serverName + CRLF
                                + "Content-Type: " + contentType + CRLF
                                + "Content-Length: " + contentLength + CRLF
                                + "Content-Range: bytes */" + contentLength + CRLF + CRLF;
                        // System.out.print(message);
                        pw.write(message);
                        pw.flush();

                        int read = 0;
                        while ((read = fileStream.read()) != -1){
                            bw.write(read);
                            // System.out.print((char) read);
                        }
                        bw.flush();
                    } else if (ranges.size() == 1){
                        // single range: only compose:
                        //  HTTP/1.1 206 Partial Content
                        //  Content-Range: bytes 21010-47021/47022
                        //  Content-Length: 26012
                        //  Content-Type: image/gif + CRLF + CRLF
                        //  ... 26012 bytes of partial image data ...

                        Integer[] range = ranges.get(0);
                        String partialHeader = protocol + " 206 Partial Content" + CRLF
                                                + "Server: " + serverName + CRLF
                                                + "Accept-Ranges: bytes" + CRLF
                                                + "Content-Type: " + contentType + CRLF
                                                + "Content-Length: " + range[2] + CRLF
                                                + "Content-Range: bytes " + range[0] + "-" + range[1] + "/" + contentLength + CRLF + CRLF;
                        pw.write(partialHeader);
                        pw.flush();
                        // System.out.print(partialHeader);
                        sendPartialMessage(fileStream, range, contentLength, 0, false);
                    } else {
                        // for loop to write 206 with bw, with Content-Range "bytes range/contentLength"
                        
                        //  HTTP/1.1 206 Partial Content
                        //  Content-Length: 1741
                        //  Content-Type: multipart/byteranges; boundary=THIS_STRING_SEPARATES

                        //  --THIS_STRING_SEPARATES
                        //  Content-Type: application/pdf
                        //  Content-Range: bytes 500-999/8000

                        //  ...the first range...
                        //  --THIS_STRING_SEPARATES
                        //  Content-Type: application/pdf
                        //  Content-Range: bytes 7000-7999/8000

                        //  ...the second range
                        //  --THIS_STRING_SEPARATES--

                        int index = 0;
                        messageSize += ranges.size() * (BOUNDARY_DELIMITER.length() + 2 + 43 + String.valueOf(contentLength).length() + contentType.length()) 
                                    + BOUNDARY_DELIMITER.length() + 2;
                        String partialHeader = protocol + " 206 Partial Content" + CRLF
                                                + "Server: " + serverName + CRLF
                                                + "Accept-Ranges: bytes" + CRLF
                                                + "Content-Length: " + messageSize + CRLF
                                                + "Content-Type: multipart/byteranges; boundary=" + BOUNDARY + CRLF + CRLF;
                        pw.write(partialHeader);
                        pw.flush();
                        // System.out.print(partialHeader);

                        for (Integer[] range : ranges){
                            pw.write(BOUNDARY_DELIMITER + CRLF);
                            // System.out.print(BOUNDARY_DELIMITER + CRLF);
                            index = sendPartialMessage(fileStream, range, contentLength, index, true);
                        }
                        pw.write(BOUNDARY_DELIMITER + DELIMITER);
                        pw.flush();
                        // System.out.print(BOUNDARY_DELIMITER + DELIMITER);
                    }
                } else {
                    // same header for GET or HEAD
                    String message = protocol + " " + reasonPhrase + CRLF
                                    + "Server: " + serverName + CRLF
                                    + "Content-Type: " + contentType + CRLF 
                                    + "Content-Length: " + contentLength + CRLF + CRLF;
                    // System.out.print(message);
                    pw.write(message);
                    pw.flush();

                    // only send messageBody if method is GET
                    if ("GET".equals(method)){
                        int read = 0;
                        while ((read = fileStream.read()) != -1){
                            bw.write(read);
                            // System.out.print((char) read);
                        }
                        bw.flush();
                    }
                }
                fileStream.close();
            }
        }

        /** helper method parse the given String request into a list of ranges 
         * 
         * @param request String of given range by parsing the range request
         * @param totalSize total size of the file
         * @return list of Integer[] - (start, end, length), return null if range is invalid
         */
        private List<Integer []> getRangesList(String request, int totalSize){
            List<Integer []> response = new ArrayList<>();
            String regexPattern = "^bytes=(\\d*-\\d*(?>,\\s*\\d*-\\d*)*)$";
            messageSize = 0;
            // the line needs to match as bytes=0-1, -500
            if (request != null && request.matches(regexPattern)){
            	Pattern patternOfRange = Pattern.compile(regexPattern);
                Matcher matchOfRange = patternOfRange.matcher(request);
                
                if (!matchOfRange.matches()) return null;
                // match is guaranteed since checked in if statement
                String[] ranges  = matchOfRange.group(1).split(","); // here, matched is a string array
                // then, get parsed group from each of the range, that's also guaranteed since initial matches succeeded
                Pattern patternOfNum = Pattern.compile("(?>\\s*)(\\d*)-(\\d*)");
                Matcher matchOfNum;
                String beginNum;
                String endNum;
                int begin;
                int end;
                int lastEnd = -1;

                // for each range, make sure they match, then parse the begin and end index
                // fill in the missng index accordingly, and check if it's valid (both >= 0, start <= end, and start > lastEnd)
                for (String range : ranges){
                	matchOfNum = patternOfNum.matcher(range);
                    if (!matchOfNum.matches()){
                    	return null;
                    } else {
                        beginNum = matchOfNum.group(1);
                        endNum = matchOfNum.group(2);

                        boolean missingBegin = (beginNum == null || beginNum.isEmpty());
                        boolean missingEnd = (endNum == null || endNum.isEmpty());

                        if (missingBegin && missingEnd){
                            return null;
                        } else if (missingEnd) {
                            begin = Integer.parseInt(beginNum);
                            end = totalSize - 1;
                        } else if (missingBegin) {
                            begin = totalSize - Integer.parseInt(endNum);
                            end = totalSize - 1;
                        } else {
                            begin = Integer.parseInt(beginNum); // inclusive
                            end = Math.min(Integer.parseInt(endNum), totalSize - 1); // inclusive
                        }

                        // range check
                        if (begin >= 0 && begin > lastEnd && begin <= end) {
                            response.add(new Integer[]{begin, end, end - begin + 1});
                            messageSize += end - begin + 1 + String.valueOf(begin).length() + String.valueOf(end).length();
                            lastEnd = end;
                        } else {return null;}
                    }
                }
            }
            return response;
        }

        /** helper method to send partial messages (header and content type was sent before calling this)
         * 
         * @param fileStream
         * @param range
         * @param contentLength
         * @param index
         * @param multi boolean value indicating whether we are sending multi range sets response
         * @return index after sending set bytes of data
         * @throws IOException
         */
        private int sendPartialMessage(FileInputStream fileStream, Integer[] range, int contentLength, int index, boolean multi) throws IOException{
            if (multi){
                String rangeHeader = "Content-Type: " + contentType + CRLF
                                   + "Content-Range: bytes " + range[0] + "-" + range[1] + "/" + contentLength + CRLF + CRLF;
                // System.out.print(rangeHeader);
                pw.write(rangeHeader);
                pw.flush();
            }
            // skip to make index be at begin index in the range
            if (index < range[0]){
                fileStream.skip(range[0] - index);
                index = range[0];
            }

            // read and write while incrementing index, index will be end + 1 when quit
            int read = 0;
            while (index <= range[1] && (read = fileStream.read()) != -1){
            	bw.write(read);
            	// System.out.print((char) read);
                index++;
            }
            bw.flush();
            
            return index;
        }
    }
}
