package flame;

import java.util.*;
import java.net.*;
import java.io.*;
import java.lang.reflect.*;

import static webserver.Server.*;
import generic.Master;
import kvs.KVSClient;
import tools.*;

class FlameMaster extends Master {

  static int nextJobID = 1;
  static HashMap<String,String> outputs;
  public static KVSClient kvs;


	public static void main(String args[]) {

    // Check the command-line arguments

    if (args.length != 2) {
      System.err.println("Syntax: FlameMaster <port> <kvsMaster>");
      System.exit(1);
    }
   
    int myPort = Integer.valueOf(args[0]);
    kvs = new KVSClient(args[1]);
    outputs = new HashMap<String,String>();

  	port(myPort);
  	registerRoutes();

    /* Set up a little info page that can be used to see the list of registered workers */

    get("/", (request,response) -> {
      response.type("text/html");
      return "<html><head><title>Flame Master</title></head><body><h3>Flame Master</h3>\n" + workerTable() + "</body></html>";
    });

    /* Set up the main route for job submissions. This is invoked from FlameSubmit. */

    post("/submit", (request,response) -> {

      // Extract the parameters from the query string. The 'class' parameter, which contains the main class, is mandatory, and
      // we'll send a 400 Bad Request error if it isn't present. The 'arg1', 'arg2', ..., arguments contain command-line
      // arguments for the job and are optional.

      String className = request.queryParams("class");
      if (className == null) {
        response.status(400, "Bad request");
        return "Missing class name (parameter 'class')";
      }
      Vector<String> argVector = new Vector<String>();
      for (int i=1; request.queryParams("arg"+i) != null; i++) 
        argVector.add(URLDecoder.decode(request.queryParams("arg"+i), "UTF-8"));
      outputs.remove(Thread.currentThread().getName());

      // We begin by uploading the JAR to each of the workers. This should be done in parallel, so we'll use a separate
      // thread for each upload.

      Thread threads[] = new Thread[getWorkers().size()];
      String results[] = new String[getWorkers().size()];
      for (int i=0; i<getWorkers().size(); i++) {
        final String url = "http://"+getWorkers().elementAt(i)+"/useJAR";
        final int j = i;
        threads[i] = new Thread("JAR upload #"+(i+1)) {
          public void run() {
            try {
              results[j] = new String(HTTP.doRequest("POST", url, request.bodyAsBytes()).body());
            } catch (Exception e) {
              results[j] = "Exception: "+e;
              e.printStackTrace();
            }
          }
        };
        threads[i].start();
      }

      // Wait for all the uploads to finish

      for (int i=0; i<threads.length; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException ie) {
        }
      }

      // Write the JAR file to a local file. Remember, we will need to invoke the 'run' method of the job, but, if the job
      // is submitted from a machine other than the master, the master won't have a local copy of the JAR file. We'll use
      // a different file name each time.

      int id = nextJobID++;
      String jarName = "job-"+id+".jar";
      File jarFile = new File(jarName);
      FileOutputStream fos = new FileOutputStream(jarFile);
      fos.write(request.bodyAsBytes());
      fos.close();

      // Load the class whose name the user has specified with the 'class' parameter, find its 'run' method, and 
      // invoke it. The parameters are 1) an instance of a FlameContext, and 2) the command-line arguments that
      // were provided in the query string above, if any. Several things can go wrong here: the class might not 
      // exist or might not have a run() method, and the method might throw an exception while it is running,
      // in which case we'll get an InvocationTargetException. We'll extract the underlying cause and report it
      // back to the user in the HTTP response, to help with debugging.
      FlameContextImpl context = new FlameContextImpl(jarName, myPort);

      try {
        Loader.invokeRunMethod(jarFile, className, context, argVector);
      } catch (IllegalAccessException iae) {
        response.status(400, "Bad request");
        return "Double-check that the class "+className+" contains a public static run(FlameContext, String[]) method, and that the class itself is public!";
      } catch (InvocationTargetException ite) {
        response.status(500, "Internal Server Error");
        ite.printStackTrace();
        StringWriter sw = new StringWriter();
        ite.getCause().printStackTrace(new PrintWriter(sw));
        return sw.toString();
      } catch (NoSuchMethodException nsme) {
        response.status(400, "Bad request");
        return "Double-check that the class "+className+" contains a public static run(FlameContext, String[]) method, and that the class itself is public!";
      } catch (Exception e){
        response.status(500, "Internal Server Error");
        e.printStackTrace();
        StringWriter sw = new StringWriter();
        e.getCause().printStackTrace(new PrintWriter(sw));
        return sw.toString();
      }

      String out;
      if (!(out = context.getOutput()).equals("")){
        return out;
      }

      return "Job finished, but produced no output";
    });
	}
}
