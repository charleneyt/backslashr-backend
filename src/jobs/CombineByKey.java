package jobs;

import static java.nio.file.StandardOpenOption.*;

import java.io.*;
import java.nio.file.*;

import kvs.Row;

public class CombineByKey {
    final static byte[] LFbyte = new byte[] { (byte) 0x0a };

	public static void main(String[] args) throws Exception {
		// args: directory for worker files
		File file = new File(args[0]);
		
		// get all tables ending in .table
        FileFilter filter = new FileFilter() {
            public boolean accept(File f) {
                return f.getName().endsWith(".table");
            }
        };
        
        File[] files = file.listFiles(filter);

        if (files == null) {
            System.out.println("Not input table found in the given directory");
            return;
        }

        FileInputStream[] currentInputStreams = new FileInputStream[files.length];
        Row[] currentBufferedRows = new Row[files.length];

        int idx = 0;
        for (File tableFile : files) {
            try {
                currentInputStreams[idx] = new FileInputStream(tableFile);
                if (currentInputStreams[idx].available() > 0){
                    Row r = Row.readFrom(currentInputStreams[idx]);
                    currentBufferedRows[idx++] = r;
                }
            } catch (Exception e) {}
        }
        
        // create output stream
        BufferedOutputStream output = new BufferedOutputStream(Files.newOutputStream(Paths.get(args[0] + "/combined.table"), CREATE));
        
        // Also get a file named dictionary.txt (we want to split into 5? under each directory)
        BufferedReader input = new BufferedReader(new FileReader(args[0]+"/dict.txt"));

        // keep reading and updating the new combined table
        String line = input.readLine();

        while (line != null) {
            line = line.trim();
            updateRowForKey(line, output, currentInputStreams, currentBufferedRows);
            // read next line
            line = input.readLine();
        }

        input.close();
	}

    // helper method will go through each Row currently saved
    // check if it's null - if so, means we finished this inputStream
    // if not null, keep rolling the Row until we get to the first row that's >= current key, and save to Row array
    // if current Row keys match, update the StringBuilder with values
    // at the end of check, if we have at least 1 match, write to file and flush
    static void updateRowForKey(String key, BufferedOutputStream output, FileInputStream[] currentInputStreams, Row[] currentBufferedRows) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < currentBufferedRows.length; i++){
            Row r = currentBufferedRows[i];
            
            // if already at the end or key still behind current, not modifying row in current round
            if (r == null || r.key().compareTo(key) > 0) continue;

            // otherwise, go ahead and row forward
            FileInputStream input = currentInputStreams[i];
            while (r != null && r.key().compareTo(key) < 0 && input.available() > 0){
                r = Row.readFrom(input);
            }
            // if we are at null or no more available stream, put null to row and close the stream (we finish this input check)
            if (r == null){
                currentBufferedRows[i] = null;
                input.close();
                currentInputStreams[i] = null;
            } else {
                currentBufferedRows[i] = r;
                if (r.key().equals(key) && r.columns().contains("value") && r.get("value").length() > 0){
                    if (sb.length() > 0){
                        sb.append(",");
                    }
                    sb.append(r.get("value"));
                }
            }
        }
        // write to file after checking all tables
        if (sb.length() > 0){
            Row returnRow = new Row(key);
            returnRow.put("value", sb.toString());
            output.write(returnRow.toByteArray());
            output.write(LFbyte);
            output.flush();
        }
    }
}
