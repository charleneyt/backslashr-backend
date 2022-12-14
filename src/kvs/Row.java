package kvs;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.nio.charset.StandardCharsets;

public class Row implements Serializable {

	protected String key;
	protected ConcurrentHashMap<String, byte[]> values;
	static boolean debugMode = false;

	public Row(String keyArg) {
		key = keyArg;
		values = new ConcurrentHashMap<String, byte[]>();
	}

	public String key() {
		return key;
	}

	public synchronized Row clone() {
		Row theClone = new Row(key);
		for (String s : values.keySet())
			theClone.values.put(s, values.get(s));
		return theClone;
	}

	public Set<String> columns() {
		return values.keySet();
	}

	public synchronized void put(String key, String value) {
		values.put(key, value.getBytes());
	}

	public synchronized void put(String key, byte[] value) {
		values.put(key, value);
	}

	public String get(String key) {
		if (values.get(key) == null)
			return null;
		return new String(values.get(key));
	}

	public byte[] getBytes(String key) {
		return values.get(key);
	}

	static String readStringSpace(InputStream in) throws Exception {
		byte buffer[] = new byte[102400];
		int numRead = 0;
		while (true) {
			if (numRead == buffer.length)
				throw new Exception("Format error: Expecting string+space");

			int b = in.read();
			if ((b < 0) || (b == 10))
				return null;
			buffer[numRead++] = (byte) b;
			if (b == ' ')
				return new String(buffer, 0, numRead - 1);
		}
	}

	public static synchronized Row readFrom(InputStream in) throws Exception {
		String theKey = readStringSpace(in);
		if (theKey == null)
			return null;

		Row newRow = new Row(theKey);
		while (true) {
			String keyOrMarker = readStringSpace(in);
			if (keyOrMarker == null)
				return newRow;

			int len = Integer.parseInt(readStringSpace(in));
			byte[] theValue = new byte[len];
			int bytesRead = 0;
			synchronized (in) {
				while (bytesRead < len) {
					int n = in.read(theValue, bytesRead, len - bytesRead);
					if (debugMode && n < 0) {
						FileWriter fw2 = new FileWriter("row_read_error_log", true);
						fw2.write("from: " + ProcessHandle.current().pid() + "| " + Thread.currentThread().getName()
								+ "\n");
						fw2.write("Premature end of stream while reading value for key '" + keyOrMarker + "' (read "
								+ bytesRead + " bytes, expecting " + len + ")" + "\n");
						fw2.close();
					}
					bytesRead += n;
				}
			}

			byte b = (byte) in.read();
			if (debugMode && b != ' ') {
				FileWriter fw = new FileWriter("row_read_error_log", true);
				fw.write("from: " + ProcessHandle.current().pid() + "| " + Thread.currentThread().getName() + "\n");
				fw.write("b is: " + b + ";len is " + len + "bytes read is " + bytesRead + "; key is: " + theKey
						+ "; value is: " + new String(theValue, StandardCharsets.UTF_8) + "\n");
				fw.close();
			}

			newRow.put(keyOrMarker, theValue);
		}
	}

	public synchronized String toString() {
		String s = key + " {";
		boolean isFirst = true;
		for (String k : values.keySet()) {
			s = s + (isFirst ? " " : ", ") + k + ": " + new String(values.get(k));
			isFirst = false;
		}
		return s + " }";
	}

	public synchronized byte[] toByteArray() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		try {
			baos.write(key.getBytes());
			baos.write(' ');

			for (String s : values.keySet()) {
				baos.write(s.getBytes());
				baos.write(' ');
				baos.write(("" + values.get(s).length).getBytes());
				baos.write(' ');
				baos.write(values.get(s));
				baos.write(' ');
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("This should not happen!");
		}
		;

		return baos.toByteArray();
	}
}