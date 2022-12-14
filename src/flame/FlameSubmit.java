package flame;

import java.net.*;
import java.io.*;

public class FlameSubmit {
	public static String submit(String server, String jarFileName, String className, String arg[]) throws Exception {
		String u = "http://" + server + "/submit" + "?class=" + className;
		for (int i = 0; i < arg.length; i++)
			u = u + "&arg" + (i + 1) + "=" + URLEncoder.encode(arg[i], "UTF-8");

		File f = new File(jarFileName);
		byte jarFile[] = new byte[(int) f.length()];
		FileInputStream fis = new FileInputStream(jarFileName);
		fis.read(jarFile);
		fis.close();

		HttpURLConnection con = (HttpURLConnection) (new URL(u)).openConnection();
		con.setRequestMethod("POST");
		con.setDoOutput(true);
		con.setFixedLengthStreamingMode(jarFile.length);
		con.setRequestProperty("Content-Type", "application/jar-archive");
		con.connect();
		OutputStream out = con.getOutputStream();
		out.write(jarFile);

		BufferedReader r = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String result = "";
		while (true) {
			String s = r.readLine();
			if (s == null)
				break;

			result = result + (result.equals("") ? "" : "\n") + s;
		}

		return result;
	}

	public static void main(String args[]) throws Exception {
		if (args.length < 3) {
			System.err.println("Syntax: FlameSubmit <server> <jarFile> <className> [args...]");
			System.exit(1);
		}

		String[] arg = new String[args.length - 3];
		for (int i = 3; i < args.length; i++)
			arg[i - 3] = args[i];

		try {
			String response = submit(args[0], args[1], args[2], arg);
			System.out.println(response);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}