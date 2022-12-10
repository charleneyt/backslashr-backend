package kvs;

import static webserver.Server.*;
import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.StandardCopyOption.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

import flame.FlameContext;
import tools.HTTP;

public class Worker extends generic.Worker {
    final static byte[] LFbyte = new byte[] { (byte) 0x0a };
    final static String LF = "\n";

    // tableName: {rowName : byteOffset}
    Map<String, ConcurrentSkipListMap<String, Long>> tablesWithOffset; // updated to save memory and reduce initial load time per Ed post #766  
    String directory;
    Map<String, BufferedOutputStream> streams;
    long lastRequestReceived;

    int workersCount;
    String nextHigherIpAndPort;
    String nextTwoHigherIpAndPort;

    String nextLowerKey;
    String nextTwoLowerKey;

    String nextLowerIpAndPort;
    String nextTwoLowerIpAndPort;
    
    boolean debugMode = false;

    public Worker(String id, int port, String masterAddr, String directory) {
        super(id, port);
        System.out.println("KVS Worker listening on " + port + " ... ");
        lastRequestReceived = System.currentTimeMillis();
        this.updateMasterIpAndPort(masterAddr);
        this.directory = directory;

        streams = new ConcurrentHashMap<>();
        tablesWithOffset = new ConcurrentHashMap<>();

//        initializeTables();
    }

    private void initializeTables() {
        File file = new File(directory);

        FileFilter filter = new FileFilter() {
            public boolean accept(File f) {
                return f.getName().endsWith(".table");
            }
        };

        File[] files = file.listFiles(filter);

        if (files == null) {
            return;
        }
        
        long startTime = System.currentTimeMillis();
        if (debugMode) {
            FileWriter fw = null;
            try {
                fw = new FileWriter("kvsWorker_log", true);
                fw.write("start writing table at: " + System.currentTimeMillis() + "\n");
                fw.close();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }           
        }


        for (File tableFile : files) {
        	if (tableFile.getName().equals("content.table"))
        		continue;
        	
            long startByte = 0;
            try {
                String tableDir = tableFile.getName();
                if (debugMode) {
                    System.out.println("initializing table " + tableDir);
                }
                FileInputStream input = new FileInputStream(tableFile);
                String tableName = tableDir.split(".table")[0];
                if (input.available() > 0) {
                    addTable(tableName);

                    while (input.available() > 0) {
                        try {
                            Row row = Row.readFrom(input);
                            if (row != null) {
                                tablesWithOffset.get(tableName).put(row.key(), startByte);
                            }
                            startByte += row.toByteArray().length + 1;
                        }
                        catch (Exception e) {
                            if (debugMode) {
                                FileWriter fw2 = new FileWriter("read_row_error_log", true);
                                fw2.write("error in initializing tables, table name is: " + tableName + "; starting byte: " + startByte + "\n");
                                fw2.close();                                
                            }
                        }
                    }
                    streams.put(tableName, new BufferedOutputStream(new FileOutputStream(tableFile, true)));
                }

            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
        }
        
        if (debugMode) {
            if (tablesWithOffset.get("domain") != null) {
                System.out.println(tablesWithOffset.get("domain").toString());              
            }
            FileWriter fw;
            try {
                fw = new FileWriter("kvsWorker_log", true);
                fw.write("end writing table at: " + System.currentTimeMillis() + "; took " + (System.currentTimeMillis() - startTime) + "\n");
                fw.close();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                return;
            }           
        }
        System.out.println(directory + "done initializing!");
    }

    public void updateRequestReceived() {
        lastRequestReceived = System.currentTimeMillis();
    }

    public synchronized void addTable(String tableName) {
        tablesWithOffset.put(tableName, new ConcurrentSkipListMap<String, Long>());
    }

    private void flush() {
        for (BufferedOutputStream stream : streams.values()) {
            try {
                stream.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    private String readStringSpace(RandomAccessFile file) throws Exception {
        byte buffer[] = new byte[1024];
        int numRead = 0;
        while (true) {
          if (numRead == buffer.length) {
              if (debugMode) {
                FileWriter fw = new FileWriter("read_row_error_log", true);
                fw.write("error in readStringSpace, not enough space in buffer, so far read is: " + buffer.toString() + "\n");
                fw.flush();
                fw.close();           
              }
              throw new Exception("Format error: Expecting string+space");  
          }

          int b = file.read();
          if ((b < 0) || (b == 10))
            return null;
          buffer[numRead++] = (byte)b;
          
          if (b == ' ')
            return new String(buffer, 0, numRead-1);
        }       
    }
    
    private synchronized Row findRow(String tableName, String rowName) {
//      System.out.println("from findRow, tablename is: " + tableName + ", rowName is: " + rowName);

        if (!tablesWithOffset.containsKey(tableName)) {
            return null;
        }
            
        if (!tablesWithOffset.get(tableName).containsKey(rowName)) {
            return null;            
        }
        
        try {
            streams.get(tableName).flush();
        } catch (IOException e3) {
            // TODO Auto-generated catch block
            e3.printStackTrace();
            try {
                FileWriter fw = new FileWriter("flush_failure", true);
                fw.write("Failed to flush table " + tableName +  "\n");
                fw.close(); 
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } 
            return null;
        }
        
        RandomAccessFile file;
        long bytePos = tablesWithOffset.get(tableName).get(rowName);
        try {
            file = new RandomAccessFile(directory + "/" + tableName + ".table", "r");
            file.seek(bytePos);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        
        String theKey;
        try {
            theKey = readStringSpace(file);
        } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            return null;
        }       

        if (theKey == null) {
            if (debugMode) {
                try {
                    FileWriter fw = new FileWriter("missing_row_log", true);
                    fw.write("From findRow, failed to find row, table name is: " + tableName + "; requested row is: " + rowName + "; bytePos is: " + bytePos +  "\n");
                    fw.close(); 
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }           
            }
            return null;
        }
        Row newRow = new Row(theKey);
        
        while (true) {
            String keyOrMarker;
            try {
                keyOrMarker = readStringSpace(file);
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                break;
            }
            if (keyOrMarker == null) {
                if (debugMode) {
                    FileWriter fw;
                    try {
                        fw = new FileWriter("find_row_log", true);
                        if (!rowName.equals(newRow.key())) {
                            fw.write("FOUND DIFFERENCE!!!\n");
                        }
                        fw.write("table name is: " + tableName + "; requested row is: " + rowName + "; returned row is: " + newRow.key() + " bytePos is: " + bytePos +  "\n");
                        fw.close();     
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                try {
                    file.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return newRow;
            }
            
            int len;
            try {
                len = Integer.parseInt(readStringSpace(file));              
            }
            catch (Exception e) {
                e.printStackTrace();
                break;
            }

            byte[] theValue = new byte[len];
            int bytesRead = 0;
            while (bytesRead < len) {
              int n;
                try {
                    n = file.read(theValue, bytesRead, len - bytesRead);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                  if (debugMode) {
                        FileWriter fw;
                        try {
                            fw = new FileWriter("read_row_error_log", true);
                            fw.write("error in findRow," +  "Premature end of stream while reading value for key '"+keyOrMarker+"' (read "+bytesRead+" bytes, expecting "+len+")" + "\n");
                            fw.close();   
                        } catch (Exception e2) {
                            // TODO Auto-generated catch block
                            e2.printStackTrace();
                            break;
                        }         
                  }
                    break;
                }
              if (n < 0) {
                  if (debugMode) {
                        FileWriter fw;
                        try {
                            fw = new FileWriter("read_row_error_log", true);
                            fw.write("error in findRow," +  "Premature end of stream while reading value for key '"+keyOrMarker+"' (read "+bytesRead+" bytes, expecting "+len+")" + "\n");
                            fw.close();   
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }         
                  }
                break;
              }
              bytesRead += n;
            }

            byte b;
            try {
                b = (byte)file.read();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                if (debugMode) {
                    FileWriter fw;
                    try {
                        fw = new FileWriter("read_row_error_log", true);
                        fw.write("error in findRow," +  "Expecting a space separator after value for key '"+keyOrMarker+"'" + "\n");
                        fw.close();
                    } catch (Exception e2) {
                        // TODO Auto-generated catch block
                        e2.printStackTrace();
                        break;
                    }                   
                }
                break;
            }
            if (b != ' ') {
                if (debugMode) {
                    FileWriter fw;
                    try {
                        fw = new FileWriter("read_row_error_log", true);
                        fw.write("error in findRow," +  "Expecting a space separator after value for key '"+keyOrMarker+"'" + "\n");
                        fw.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }                   
                }
                break;              
            }

            newRow.put(keyOrMarker, theValue);
        }
        
        try {
            file.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
    
    private String findCol(Row row, String colName) {
        return row.get(colName);
    }
    
    private synchronized static byte[] toByteArrayWithNewLine(byte[] rowBytes)  {
        byte[] result = Arrays.copyOf(rowBytes, rowBytes.length + LFbyte.length);
        System.arraycopy(LFbyte, 0, result, rowBytes.length, LFbyte.length);
        return result;
      }
    
    private String getProcessThreadName() {
    	return ProcessHandle.current().pid() + "| " + Thread.currentThread().getName();
    }

    // Accept 3 command line args: 1) a port number for the worker, 2) a storage
    // directory, and 3) the IP and port of the master, separated by a colon (:).
    // When started, this application should look for a file called id in the
    // storage directory; if it exists, it should read the worker’s ID from this
    // file, otherwise it should pick an ID of five random lower-case letters and
    // write it to the file.
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Args should be 3!");
            return;
        }
        int port = Integer.valueOf(args[0]);
        if (!Files.isDirectory(Paths.get(args[1]))) {
            System.out.println("Given directory is not valid");
            return;
        }

        String masterAddr = args[2];
        if (!masterAddr.contains(":")) {
            System.out.println("Master address should be ip:port");
            return;
        }

        String id;
        Path dir = Paths.get(args[1]);
        if (!Files.isDirectory(dir)) {
            System.out.println("Args[1] should be a directory");
            return;
        }
        Path idFile = Paths.get(args[1] + "/id");
        if (Files.exists(idFile) && Files.isReadable(idFile)) {
            id = Files.readAllLines(idFile).get(0);
        } else {
            id = generateId(5);
            Files.write(idFile, id.getBytes());
        }

        port(port);
        Worker worker = new Worker(id, port, masterAddr, args[1]);

        // It should make a /ping request to the master every five seconds
        worker.startPingThread();
        
//        get("/data/dictionary/:word", (req, res) -> {
//        	if (worker.debugMode) {
//	        	FileWriter fw = new FileWriter("check_dict_threads_log" + worker.directory, true);
//	        	fw.write("from check dict: " + worker.getProcessThreadName() + "word is: " + req.params("word") + "\n");
//	        	fw.flush();
//	        	System.out.println("from check dict: " + worker.getProcessThreadName() + "word is: " + req.params("word"));        		
//        	}
//
//        	String word = req.params("word");
//        	
//        	if (!worker.dictionary.contains(word)) {
//                res.status(404, "Not Found");
//                return "404 Not Found";        		
//        	}
//        	
//        	res.status(200, "OK");
//        	res.body("OK");
//        	
//        	if (worker.debugMode) {
//        		FileWriter fw = new FileWriter("check_dict_threads_log" + worker.directory, true);
//	        	fw.write("end check dict: " + worker.getProcessThreadName() + "word is: " + req.params("word") + "\n");
//	        	fw.close();
//	        	System.out.println("end check dict: " + worker.getProcessThreadName() + "word is: " + req.params("word")); 		
//        	}
//
//        	return null;
//        });
        
        // remove aged rows from the file
        put("/clean/:table", (req, res) -> {
        	synchronized(worker.tablesWithOffset) {
//        		if (worker.debugMode) {
//		        	System.out.println("from clean: " + worker.getProcessThreadName());        			
//        		}

	        	String tableName = req.params("table");
	        	
	        	if (!worker.tablesWithOffset.containsKey(tableName)) {
	                res.status(404, "Not Found");
	                return "404 Not Found";        		
	        	}
	        	
				Path currFile = Paths.get(worker.directory + "/" + tableName + ".table");
				Path tempFile = Paths.get(worker.directory + "/" + tableName + ".table1");
				
				// write current row in the table to tempFile
				BufferedOutputStream newStream = new BufferedOutputStream(Files.newOutputStream(tempFile, CREATE, APPEND));
				long offSetCounter = 0;
				ConcurrentSkipListMap<String, Long> newRowToOffSets = new ConcurrentSkipListMap<>();
				for (String rowName : worker.tablesWithOffset.get(tableName).keySet()) {
					Row row = worker.findRow(tableName, rowName);
					if (row == null)
						continue;
					
					synchronized(newStream) {
						newStream.write(toByteArrayWithNewLine(row.toByteArray()));
					}
					newRowToOffSets.put(rowName, offSetCounter);
					offSetCounter += row.toByteArray().length + 1;
				}
				// close the streams
				newStream.close();
				worker.streams.get(tableName).close();
	
				// atomically replace the current log file
				// also update the offset mapping
				Files.move(tempFile, currFile, ATOMIC_MOVE, REPLACE_EXISTING);
				worker.tablesWithOffset.put(tableName, newRowToOffSets);
//				System.out.println("new mapping: " + newRowToOffSets.toString());
	
				// update the stream in streams
				worker.streams.put(tableName, new BufferedOutputStream(Files.newOutputStream(currFile, CREATE, APPEND)));
//				if (worker.debugMode) {
//		        	System.out.println("end clean: " + worker.getProcessThreadName());					
//				}
				return "OK";        		
        	}

        });


        // PUT /data/<T>/<R>/<C> should set column C in row R of table T to the
        // (possibly binary) data in the body of the request
        put("/data/:table/:row/:col", (req, res) -> {
        	synchronized(worker.tablesWithOffset) {
//            	System.out.println("from put: " + worker.getProcessThreadName());        	
                String tableName = req.params("table");
                String rowName = req.params("row");
                String colName = req.params("col");
                if (!worker.tablesWithOffset.containsKey(tableName)) {
                    worker.addTable(tableName);
                }

                if (!worker.streams.containsKey(tableName)) {
                    Path outputFile = Paths.get(args[1] + "/" + tableName + ".table");
                    worker.streams.put(tableName,
                            new BufferedOutputStream(Files.newOutputStream(outputFile, CREATE, APPEND)));
                }
                BufferedOutputStream currStream = worker.streams.get(tableName);

                Map<String, Long> currTable = worker.tablesWithOffset.get(tableName);
                Row row;
                if (!currTable.containsKey(rowName)) {
                    row = new Row(rowName);
                } else {
                    row = worker.findRow(tableName, rowName);
                }
                
                if (row == null) {
                    if (worker.debugMode) {
                        FileWriter fw = new FileWriter("missing_row_log", true);
                        fw.write("Table: " + tableName +", row: " + rowName + ", colName: " + colName + "\n");
                        fw.close();                 
                    }

                    row = new Row(rowName);
                }
                
                currStream.flush();
                long currByteCount = Files.size(Paths.get(worker.directory, tableName + ".table"));
                
                currTable.put(rowName, currByteCount);
                row.put(colName, req.bodyAsBytes());
                
                synchronized(currStream) {
                	currStream.write(toByteArrayWithNewLine(row.toByteArray()));
                };
                	
                currStream.flush();
                
                if (worker.debugMode) {
                    FileWriter fw = new FileWriter("put_row_log", true);
                    fw.write("put /data/:table/:row/:col: table is: " + tableName + ", row is: " + rowName + ", col is: " + colName + "at time " + System.currentTimeMillis() + "\n");
                    fw.close();             
                }
//            	System.out.println("end put: " + worker.getProcessThreadName()); 
            	
                return "OK";
        	}
//        	System.out.println("from put: " + worker.getProcessThreadName());        	
//            String tableName = req.params("table");
//            String rowName = req.params("row");
//            String colName = req.params("col");
//          System.out.println("from put /data/:table/:row/:col, table name is: " + tableName + ", rowName is: " + rowName + ", colName is: " + colName + "\n");
            
//            // EC 1
//            worker.updateRequestReceived();
//
//            if (req.queryParams() != null && !req.queryParams().contains("forwarded") && worker.workersCount >= 3) {
//                HTTP.doRequest("PUT",
//                        "http://" + worker.nextHigherIpAndPort + "/data/"
//                                + java.net.URLEncoder.encode(tableName, "UTF-8") + "/"
//                                + java.net.URLEncoder.encode(rowName, "UTF-8") + "/"
//                                + java.net.URLEncoder.encode(colName, "UTF-8") + "?forwarded=true",
//                        req.bodyAsBytes());
//                HTTP.doRequest("PUT",
//                        "http://" + worker.nextTwoHigherIpAndPort + "/data/"
//                                + java.net.URLEncoder.encode(tableName, "UTF-8") + "/"
//                                + java.net.URLEncoder.encode(rowName, "UTF-8") + "/"
//                                + java.net.URLEncoder.encode(colName, "UTF-8") + "?forwarded=true",
//                        req.bodyAsBytes());
//            }
            
//            worker.putAndClean(tableName, rowName, colName, args[1]);
            
            
//            if (!worker.tablesWithOffset.containsKey(tableName)) {
//                worker.addTable(tableName);
//            }
//
//            if (!worker.streams.containsKey(tableName)) {
//                Path outputFile = Paths.get(args[1] + "/" + tableName + ".table");
//                worker.streams.put(tableName,
//                        new BufferedOutputStream(Files.newOutputStream(outputFile, CREATE, APPEND)));
//            }
//            BufferedOutputStream currStream = worker.streams.get(tableName);
//
//            Map<String, Long> currTable = worker.tablesWithOffset.get(tableName);
//            Row row;
//            if (!currTable.containsKey(rowName)) {
//                row = new Row(rowName);
//            } else {
//                row = worker.findRow(tableName, rowName);
//            }
//            
//            if (row == null) {
//                if (worker.debugMode) {
//                    FileWriter fw = new FileWriter("missing_row_log", true);
//                    fw.write("Table: " + tableName +", row: " + rowName + ", colName: " + colName + "\n");
//                    fw.close();                 
//                }
//
//                row = new Row(rowName);
//            }
//            
//            currStream.flush();
//            long currByteCount = Files.size(Paths.get(worker.directory, tableName + ".table"));
//            
//            currTable.put(rowName, currByteCount);
//            row.put(colName, req.bodyAsBytes());
//            
//            synchronized(currStream) {
//            	currStream.write(toByteArrayWithNewLine(row.toByteArray()));
//            };
//            	
//            currStream.flush();
//            
//            if (worker.debugMode) {
//                FileWriter fw = new FileWriter("put_row_log", true);
//                fw.write("put /data/:table/:row/:col: table is: " + tableName + ", row is: " + rowName + ", col is: " + colName + "at time " + System.currentTimeMillis() + "\n");
//                fw.close();             
//            }
//        	System.out.println("end put: " + worker.getProcessThreadName()); 
//        	
//        	System.out.println("from clean: " + worker.getProcessThreadName());
//        	
//        	if (!worker.tablesWithOffset.containsKey(tableName)) {
//                res.status(404, "Not Found");
//                return "404 Not Found";        		
//        	}
//        	
//			Path currFile = Paths.get(worker.directory + "/" + tableName + ".table");
//			Path tempFile = Paths.get(worker.directory + "/" + tableName + ".table1");
//			
//			// write current row in the table to tempFile
//			BufferedOutputStream newStream = new BufferedOutputStream(Files.newOutputStream(tempFile, CREATE, APPEND));
//			long offSetCounter = 0;
//			ConcurrentSkipListMap<String, Long> newRowToOffSets = new ConcurrentSkipListMap<>();
//			for (String name : worker.tablesWithOffset.get(tableName).keySet()) {
//				Row currRow = worker.findRow(tableName, name);
//				if (currRow == null)
//					continue;
//				
//				synchronized(newStream) {
//					newStream.write(toByteArrayWithNewLine(currRow.toByteArray()));
//				}
//				newRowToOffSets.put(name, offSetCounter);
//				offSetCounter += currRow.toByteArray().length + 1;
//			}
//			// close the streams
//			newStream.close();
//			worker.streams.get(tableName).close();
//
//			// atomically replace the current log file
//			// also update the offset mapping
//			Files.move(tempFile, currFile, ATOMIC_MOVE, REPLACE_EXISTING);
//			worker.tablesWithOffset.put(tableName, newRowToOffSets);
//			System.out.println("new mapping: " + newRowToOffSets.toString());
//
//			// update the stream in streams
//			worker.streams.put(tableName, new BufferedOutputStream(Files.newOutputStream(currFile, CREATE, APPEND)));
//        	System.out.println("end clean: " + worker.getProcessThreadName());
//        	
//            return "OK";
        });

        // GET /data/<T>/<R>/<C> should return the data in column C of row R in table T
        // if the table, row, and column all exist; if not, it should return a 404 error
        get("/data/:table/:row/:col", (req, res) -> {
            String tableName = req.params("table");
            String rowName = req.params("row");
            String colName = req.params("col");

            // EC 1
            worker.updateRequestReceived();
            
//          System.out.println("from get /data/:table/:row/:col, " + tableName + ", " + rowName +", " + colName + "\n");

            Row targetRow = worker.findRow(tableName, rowName);
            if (targetRow == null) {
                res.status(404, "Not Found");
                return "404 Not Found";             
            }
            
            String targetCol = worker.findCol(targetRow, colName);
            if (targetCol == null) {
                res.status(404, "Not Found");
                return "404 Not Found";             
            }

            res.bodyAsBytes(targetCol.getBytes());
            return null;
        });

        // return HTML pages (content type text/html. The first route should return a
        // HTML table with a row for each data table on the worker; each row should
        // contain a) the name of the table – say, XXX – with a hyperlink to /view/XXX,
        // and b) the number of keys in the table.
        get("/", (req, res) -> {
            // EC 1
            worker.updateRequestReceived();

            res.type("text/html");
            StringBuilder sb = new StringBuilder();
            sb.append("<html>");
            sb.append("<style> tr, th, td {border: 1px solid black;}</style>");
            sb.append("<br>");
            sb.append("<table><tr><td>Table Name</td><td>Number of Row Keys</td></tr>");
            for (Map.Entry<String, ConcurrentSkipListMap<String, Long>> tableEntry : worker.tablesWithOffset.entrySet()) {
                String link = "/view/" + tableEntry.getKey();
                sb.append("<tr><td><a href=\"" + link + "\">" + tableEntry.getKey() + "</td><td>"
                        + tableEntry.getValue().size() + "</td></tr>");
            }           

            sb.append("</table>");
            return "<!doctype html><html><head><title>KVS Client - List of All Tables</title></head><body><div>KVS Client - List of All Tables</div>"
                    + sb.toString() + "</body></html>";
        });

        // return a HTML page with 10 rows of data; it should have one HTML row for each
        // row of data, one column for the row key, and one column for each column name
        // that appears at least once in those 10 rows. The cells should contain the
        // values in the relevant rows and columns, or be empty if the row does not
        // contain a column with that name. The rows should be sorted by column key. If
        // the data table contains more than 10 rows, the route should display the first
        // 10, and there should be a “Next” link at the end of the table that displays
        // another table with the next 10 rows. You may add query parameters to the
        // route for this purpose.
        get("/view/:table", (req, res) -> {
            String tableName = req.params("table");

            // EC 1
            worker.updateRequestReceived();
            
            if (!worker.tablesWithOffset.containsKey(tableName)) {
                res.status(404, "Not Found");
                return "404 Not Found";
            }
            
            ConcurrentSkipListMap<String, Long> currTable = worker.tablesWithOffset.get(tableName);

            if (currTable.size() < 1) {
                res.type("text/html");
                return "<!doctype html><html><head><title>KVS Client - Table " + tableName
                        + "</title></head><body><div>KVS Client - Table " + tableName + "</div>"
                        + "<table>Table is empty!</table>" + "</body></html>";
            }

            // get the sorted key in list
            List<String> rowsName = new ArrayList<>(currTable.navigableKeySet());

            // get the rows starting index, by default it's 0, or if query param has
            // startRow
            String startRow = rowsName.get(0);
            int startIndex = 0;
            if (req.queryParams() != null && req.queryParams().contains("startRow")) {
                startRow = req.queryParams("startRow");
                startIndex = rowsName.indexOf(startRow);
                if (startIndex == -1) {
                    res.status(404, "Not Found");
                    return "404 Not Found";
                }
            }
            // calculating endIndex to get a submap for better runtime, 10 rows max
            int endIndex = Math.min(rowsName.size() - 1, startIndex + 10);
            NavigableMap<String, Long> subTable = currTable.subMap(startRow, true, rowsName.get(endIndex), true);

            // collect the column names in the 10 rows (or less if currently less than 10)
            Set<String> columnVals = new HashSet<>();
            for (String rowName : subTable.keySet()) {
                Row row = worker.findRow(tableName, rowName);
                if (row != null)
                    columnVals.addAll(row.columns());
            }

            // remove page content in UI
//          columnVals.remove("page");

            // inevitable work to sort the columns in current 10 rows!
            List<String> colsName = new ArrayList<>(columnVals);
            colsName.sort((a, b) -> a.compareTo(b));

            // start to make table!
            // first line
            StringBuilder sb = new StringBuilder();
            sb.append("<html>");
            sb.append("<style> tr, th, td {border: 1px solid black;}</style>");
            sb.append("<br>");
            sb.append("<table><tr><td>Row</td>");
            for (String col : colsName) {
                sb.append("<td>" + col + "</td>");
            }
            sb.append("</tr>");
            
            // every row
            for (String rowName : subTable.keySet()) {
                sb.append("<tr><td>" + rowName + "</td>");
                Row row = worker.findRow(tableName, rowName);
                for (String col : colsName) {
                    if (row.columns().contains(col)) {
//                      if (col.equals("page")) {
//                          continue;
//                      }
                        sb.append("<td>" + row.get(col) + "</td>");
                    } else {
                        sb.append("<td></td>");
                    }
                }
                sb.append("</tr>");
            }
            sb.append("</table>");

            // generate a Next link with startRow if we have more in the table
            if (endIndex + 1 < rowsName.size()) {
                String link = "/view/" + tableName + "?startRow=" + rowsName.get(endIndex + 1);
                sb.append("<div><a href=\"" + link + "\">" + "Next</div>");
            }

            res.type("text/html");
            return "<!doctype html><html><head><title>KVS Client - Table " + tableName
                    + "</title></head><body><div>KVS Client - Table " + tableName + "</div>" + sb.toString()
                    + "</body></html>";
            
        });

        // GET route for /data/XXX/YYY, where XXX is a table name and YYY is a row key.
        // If table XXX exists and contains a row with key YYY, the worker should
        // serialize this row using Row.toByteArray() and send it back in the body of
        // the response. If the table does not exist or does not contain a row with the
        // relevant key, it should return a 404 error code.
        get("/data/:table/:row", (req, res) -> {
            String tableName = req.params("table");
            String rowName = req.params("row");

            // EC 1
            worker.updateRequestReceived();
//            System.out.println("url is: " + req.url());
//            System.out.println("before find row");
            Row targetRow = worker.findRow(tableName, rowName);
//            System.out.println("after find row");
            if (targetRow != null) {
                res.bodyAsBytes(targetRow.toByteArray());
                return null;
            }
            else {
                res.status(404, "Not Found");
                return "404 Not Found";
            }
        });

        // iterate over all the local entries in table XXX, serialize each entry with
        // Row.toByteArray() and then send the entries back, each followed by a LF
        // character (ASCII code 10). After the last entry, there should be another LF
        // character to indicate the end of the stream.
        get("/data/:table", (req, res) -> {
//          System.out.println("/data/:table"+ req.queryParams()+" " + req.url());
            String tableName = req.params("table");

            // EC 1
            worker.updateRequestReceived();
            
            // check if the table exist         
            if (!worker.tablesWithOffset.containsKey(tableName)) {
                res.status(404, "Not Found");
                return "404 Not Found";
            }

            // check for startRow and endRowExclusive in query params
            boolean hasStartRow = req.queryParams() != null && req.queryParams().contains("startRow");
            String startRow = "";
            if (hasStartRow) {
                startRow = req.queryParams("startRow");
            }
            boolean hasEndRow = req.queryParams() != null && req.queryParams().contains("endRowExclusive");
            String endRow = "";
            if (hasEndRow) {
                endRow = req.queryParams("endRowExclusive");
            }

            if (hasStartRow && hasEndRow && startRow.compareTo(endRow) > 0) {
                res.status(400, "Bad Request");
                return "400 Bad Request (startRow cannot be larger than endRowExclusive!)";
            }
            
//          System.out.println("endRow: "+endRow);
            Collection<String> rowSet = worker.tablesWithOffset.get(tableName).keySet();
            int rowCount = 0;
            boolean beginning = false;
            for (String rowName : rowSet) {
//              System.out.println("rowName: "+rowName);
                Row row = worker.findRow(tableName, rowName);
                if (row == null) {
//                  System.out.println("from /data/:table " + rowName + "is null!!\n");
                    continue;
                }
                if (!hasStartRow || beginning || rowName.compareTo(startRow) >= 0) {
                    beginning = true;
                    if (!hasEndRow || rowName.compareTo(endRow) < 0) {
//                      System.out.println("row.toByteArray(): "+row.toString());
                        res.write(toByteArrayWithNewLine(row.toByteArray()));
//                      res.write(Worker.LFbyte);
                        rowCount++;
                    } else {
                        break;
                    }
                }
            }

            res.write(Worker.LFbyte);
            // in case no row met the query
            if (rowCount == 0) {
                res.write(Worker.LFbyte);
            }
            return null;

        });
        
//        for (File tableFile : files) {
//        	if (tableFile.getName().equals("content.table"))
//        		continue;
//        	
//            long startByte = 0;
//            try {
//                String tableDir = tableFile.getName();
//                if (debugMode) {
//                    System.out.println("initializing table " + tableDir);
//                }
//                FileInputStream input = new FileInputStream(tableFile);
//                String tableName = tableDir.split(".table")[0];
//                if (input.available() > 0) {
//                    addTable(tableName);
//
//                    while (input.available() > 0) {
//                        try {
//                            Row row = Row.readFrom(input);
//                            if (row != null) {
//                                tablesWithOffset.get(tableName).put(row.key(), startByte);
//                            }
//                            startByte += row.toByteArray().length + 1;
//                        }
//                        catch (Exception e) {
//                            if (debugMode) {
//                                FileWriter fw2 = new FileWriter("read_row_error_log", true);
//                                fw2.write("error in initializing tables, table name is: " + tableName + "; starting byte: " + startByte + "\n");
//                                fw2.close();                                
//                            }
//                        }
//                    }
//                    streams.put(tableName, new BufferedOutputStream(new FileOutputStream(tableFile, true)));
//                }
//
//            } catch (Exception e) {
//                e.printStackTrace();
//                continue;
//            }
//        }
        
        get("/raw/:table", (req, res) -> {
        	
          System.out.println("/raw/:table started");
            String tableName = req.params("table");
                          
            FileInputStream input = new FileInputStream(worker.directory + "/" + tableName + ".table");
            
            if (input.available() <= 0) {
                res.status(404, "Not Found");
                input.close();
                return "404 Not Found";
            }

            // check for startRow and endRowExclusive in query params
            boolean hasStartRow = req.queryParams() != null && req.queryParams().contains("startRow");
            String startRow = "";
            if (hasStartRow) {
                startRow = req.queryParams("startRow");
            }
            boolean hasEndRow = req.queryParams() != null && req.queryParams().contains("endRowExclusive");
            String endRow = "";
            if (hasEndRow) {
                endRow = req.queryParams("endRowExclusive");
            }

            if (hasStartRow && hasEndRow && startRow.compareTo(endRow) > 0) {
                res.status(400, "Bad Request");
                return "400 Bad Request (startRow cannot be larger than endRowExclusive!)";
            }
            
            int rowCount = 0;
            boolean beginning = false;
            while (input.available() > 0) {
                try {
                    Row row = Row.readFrom(input);
                    if (row == null)
                    	continue;
	                if (!hasStartRow || beginning || row.key().compareTo(startRow) >= 0) {
	                    beginning = true;                    
//	                	System.out.println("from get raw, row is: " + row.toString());
	                	rowCount++;
	                	System.out.println("row read is: " + row.toString());
	                    res.write(toByteArrayWithNewLine(row.toByteArray()));        
	                } else {
	                	break;
	                }
                }
                catch (Exception e) {
                	e.printStackTrace();
                }
            }
       

	            
////	//          System.out.println("endRow: "+endRow);
////	            Collection<String> rowSet = worker.tablesWithOffset.get(tableName).keySet();
//	            int rowCount = 0;
//	            boolean beginning = false;
//	            for (String rowName : rowSet) {
//	//              System.out.println("rowName: "+rowName);
//	                Row row = worker.findRow(tableName, rowName);
//	                if (row == null) {
//	//                  System.out.println("from /data/:table " + rowName + "is null!!\n");
//	                    continue;
//	                }
//	                if (!hasStartRow || beginning || rowName.compareTo(startRow) >= 0) {
//	                    beginning = true;
//	                    if (!hasEndRow || rowName.compareTo(endRow) < 0) {
//	//                      System.out.println("row.toByteArray(): "+row.toString());
//	                        res.write(toByteArrayWithNewLine(row.toByteArray()));
//	//                      res.write(Worker.LFbyte);
//	                        rowCount++;
//	                    } else {
//	                        break;
//	                    }
//	                }
//	            }
//	
            res.write(Worker.LFbyte);
            // in case no row met the query
            if (rowCount == 0) {
                res.write(Worker.LFbyte);
            }
            System.out.println("/raw/:table ended");
            return null;

        });

        // the body will contain one or multiple rows, separated by a LF character. The
        // worker should read the rows one by one and insert them into table XXX;
        // existing entries with the same key should be overwritten.
        put("/data/:table", (req, res) -> {
        	synchronized(worker.tablesWithOffset) {
//	        	System.out.println("from /data/:table");
	            String tableName = req.params("table");
	
	            if (!worker.tablesWithOffset.containsKey(tableName)) {
	                worker.addTable(tableName);
	            }
	
	            if (!worker.streams.containsKey(tableName)) {
	                Path outputFile = Paths.get(args[1] + "/" + tableName + ".table");
	                worker.streams.put(tableName,
	                        new BufferedOutputStream(Files.newOutputStream(outputFile, CREATE, APPEND)));
	            }
	            BufferedOutputStream currStream = worker.streams.get(tableName);
	
	            InputStream input = new ByteArrayInputStream(req.bodyAsBytes());
	            FileWriter fw = new FileWriter("put_data_table_log", true);
	            fw.write(req.body());
	            fw.close();
	            if (!worker.tablesWithOffset.containsKey(tableName)) {
	                worker.addTable(tableName);
	            }
	            
            	currStream.flush();	 
	            long bytesRead = Files.size(Paths.get(worker.directory, tableName + ".table"));
	            while (input.available() > 0) {
	                Row row = Row.readFrom(input);
	                if (row != null) {
	                	worker.tablesWithOffset.get(tableName).put(row.key(), bytesRead);	            
	                	bytesRead += row.toByteArray().length + 1;
	                	
//	                	String rowName = row.key();
//	                	Row existingRow = worker.findRow(tableName, rowName);
//	                	if (existingRow == null) {
//		                    worker.tablesWithOffset.get(tableName).put(row.key(), bytesRead);	                		
//	                	}
//	                	else {
//	                		String existingPos = existingRow.get("value");
//	                		String currPos = existingPos + "," + row.get("value");
//	                		row.put("value", currPos);
//	                	}
//	                    worker.tablesWithOffset.get(tableName).put(row.key(), bytesRead);
	                    
	                    
	                    synchronized(currStream) {
	                    	currStream.write(toByteArrayWithNewLine(row.toByteArray()));
	                    }
	                }
//		                else {
//		                	System.out.println("row is null");
//		                }
	            }
	            currStream.flush();	           
	            
	            return "OK";        		
        	}

        });

        // the body will contain another name YYY, and the worker should rename table
        // XXX to YYY (and the corresponding log file from XXX.table to YYY.table. The
        // worker should return a 404 status if table XXX is not found, and a 409 status
        // if table YYY already exists.
        put("/rename/:table", (req, res) -> {
            String oldTable = req.params("table");
            String newTable = req.body();

            // EC 1
            worker.updateRequestReceived();

            if (!worker.tablesWithOffset.containsKey(oldTable)) {
                res.status(404, "Not Found");
                return "404 Not Found";
            }
            if (worker.tablesWithOffset.containsKey(newTable)) {
                res.status(409, "Conflict");
                return "409 Conflict";
            }

            Path oldFile = Paths.get(args[1] + "/" + oldTable + ".table");
            Path newFile = Paths.get(args[1] + "/" + newTable + ".table");

            // close the stream, then remove it
            worker.streams.get(oldTable).close();
            worker.streams.remove(oldTable);
            // rename the file (replace existing in case a .table exists but has not
            // associated table entry?) all tables should have .table and all .table should
            // have table. Not necessary for REPLACE_EXISTING
            Files.move(oldFile, newFile, ATOMIC_MOVE, REPLACE_EXISTING);
            // open the new stream for new table name
            worker.streams.put(newTable, new BufferedOutputStream(Files.newOutputStream(newFile, CREATE, APPEND)));
            
            // move the table from old to new key
            worker.tablesWithOffset.put(newTable, worker.tablesWithOffset.get(oldTable));
            // delete old entry
            worker.tablesWithOffset.remove(oldTable);

            return "OK";
        });

        // add the route to delete the table for worker
        put("/delete/:table", (req, res) -> {
            String tableToDelete = req.params("table");
            
            if (!worker.tablesWithOffset.containsKey(tableToDelete)) {
                res.status(404, "Not Found");
                return "404 Not Found";
            }

            Path fileToDelete = Paths.get(args[1] + "/" + tableToDelete + ".table");

            // close the stream, then remove it
            worker.streams.get(tableToDelete).close();
            worker.streams.remove(tableToDelete);

            // delete the file
            Files.deleteIfExists(fileToDelete);
            
            // delete old entry
            worker.tablesWithOffset.remove(tableToDelete);

            return "OK";
        });

        // If a table with this name exists, the body of the response should contain the
        // number of rows in that table (as an ASCII string); otherwise, you should send
        // a 404 error code.
        get("/count/:table", (req, res) -> {
            String tableName = req.params("table");

            // EC 1
            worker.updateRequestReceived();
            
            if (!worker.tablesWithOffset.containsKey(tableName)) {
                res.status(404, "Not Found");
                return "404 Not Found";
            } else {
                int size = worker.tablesWithOffset.get(tableName).size();
                res.body(String.valueOf(size));
                return null;
            }
        });
    }
}
