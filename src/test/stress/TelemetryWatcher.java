/*
 * Copyright 2003-2016 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Current instance of TelemetryWatcher class has 8 classes 
// and 1 interface defined inside it.  
// This goes against usual Best Practicies
// TelemetryWatcher class should be split in several files.


package test.stress;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.Headers;
import java.util.concurrent.Executors;

import test.telemetry.TelemetryServer;
import test.telemetry.TelemetryReader;
import test.telemetry.TelemetryObserver;
import test.utilities.StressCmdHandler;
import test.utilities.StressCmdProcessor;


public class
TelemetryWatcher
  implements TelemetryObserver,
    StressCmdHandler
{
  public static final int DEFAULT_PORT = 26669;
  public static final int DEFAULT_MAX_CONNECTIONS = 20;
  public static final int DEFAULT_REFRESH_COUNT = 30;

  private String localHostName = "localhost";
  private String telemetryHost = "localhost";
  private int telemetryPort = TelemetryServer.DEFAULT_TELEMETRY_PORT;
  private TelemetryReader telemetry = null;
  private int httpPort = DEFAULT_PORT;
  private volatile int refreshCount = DEFAULT_REFRESH_COUNT;
  private boolean doShutdown = false;
  private volatile boolean isInitialized = false;

  private SimpleDateFormat dateFormatter;

  private Service oldHttpServer = null;

  private HttpServer httpServer = null;

  private Hashtable<String, String> telemetryData;


	Map services;				// Hashtable mapping ports to Listeners
	Set connections;			// the set of current connections
	int maxConnections;			// the concurrent connection limit
	ThreadGroup threadGroup;	// the threadgroup for all our threads
	PrintWriter logStream;		// where we send our logging output
	Logger logger;				// a Java 1.4 logging destination
	Level logLevel;				// the level to log messages at

  public TelemetryWatcher() {
    dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    telemetryData = new Hashtable<String, String>();
    maxConnections = DEFAULT_MAX_CONNECTIONS;
  }

  public TelemetryWatcher(String host, int port) {
    this();
    telemetryHost = host;
    telemetryPort = port;
  }

  public synchronized void initialize()
    throws Exception {

    if (isInitialized)
      return;

    if (services == null)
      services = new HashMap();
    if (connections == null)
      connections = new HashSet(maxConnections);

    localHostName = InetAddress.getLocalHost().getHostName();

    // first see if we can connect to the server
    telemetry = new TelemetryReader(telemetryHost, telemetryPort);
    telemetry.addTelemetryObserver(this);
    telemetry.initialize();
    telemetry.start();

    // if we're internal to the stress manager, we should be able to communicate
    StressCmdProcessor cmdProcessor = StressManager.getCmdProcessor();
    if (cmdProcessor != null) {
      cmdProcessor.addHandler(this);
    }

    initHttpServer();

    isInitialized = true;
  }

  public void shutdown()
    throws Exception {

    if ( telemetry != null)
      telemetry.removeTelemetryObserver(this);
    if (httpServer != null)
      httpServer.stop(30);

    // if we're internal to the stress manager, we should be able to communicate
    StressCmdProcessor cmdProcessor = StressManager.getCmdProcessor();
    if (cmdProcessor != null) {
      cmdProcessor.removeHandler(this);
    }
    
  }

  public void
  setTelemetryHost(String host) {
    telemetryHost = host;
  }

  public void
  setTelemetryPort(int port) {
    telemetryPort = port;
  }

  public void
  setHttpPort(int port) {
    if (!isInitialized)
      httpPort = port;
  }

	public
	TelemetryWatcher(OutputStream logStream, int maxConnections) {
		this(maxConnections);
		setLogStream(logStream);
		log("Starting server");
	}

	/**
	 * 	contructor to use to support logging with Java 1.4 logger
	 */
	public
	TelemetryWatcher(Logger logger, Level logLevel, int maxConnections) {
		this(maxConnections);
		setLogger(logger, logLevel);
		log("Starting server");
	}

	public
	TelemetryWatcher(int maxConnections) {
    this();
		threadGroup = new ThreadGroup(TelemetryWatcher.class.getName());
		this.maxConnections = maxConnections;
		services = new HashMap();
		connections = new HashSet(maxConnections);
	}

	public synchronized void
	setLogStream(OutputStream out) {
		if (out != null)
			logStream = new PrintWriter(out);
		else
			logStream = null;
	}

	public synchronized void
	setLogger(Logger logger, Level level) {
		this.logger = logger;
		this.logLevel = level;
	}

	/** write the specified string to the log */
	protected synchronized void
	log(String s) {
		if (logger != null)
			logger.log(logLevel, s);
		if (logStream != null) {
			logStream.println("[" + dateFormatter.format(new Date()) + "] " + s);
			logStream.flush();
		}
	}

	/** write the specified object to the log */
	protected void log(Object o) {
		log(o.toString());
	}

  private boolean stripTrailingColon = false;

  public void
  telemetryEvent(String key, String value) {
    // trim off the trailing colon -
    // this is no longer necessary. It's being handled within the
    // reader code (as it should have been all along)
    // keeping track of this right now lest everything break
    if (stripTrailingColon) {
    String mykey = key;
    int pos = key.indexOf(":");
    if (pos > 0)
      mykey = key.substring(0, pos);
    telemetryData.put(mykey, value);
    } else {
      telemetryData.put(key, value);
    }
  }

	/**
	 * this method makes the server start providing a new service
	 * it runs the specified Service object on the specified port
	 */
	public synchronized void
	addService(Service service, int port)
		throws IOException {
		Integer key = new Integer(port);
		// check whether a service is already on that port
		if (services.get(key) != null)
			throw new IllegalArgumentException("Port " + port + 
						" already in use");
		// create a Listener object to listen for connections on the port
		Listener listener = new Listener(threadGroup, port, service);
		services.put(key, listener);
		// log the event
		log("Starting service " + service.getClass().getName() +
				" on port " + port);
		// start the listener
		listener.start();
	}

	public synchronized void
	removeService(int port) {
		Integer key = new Integer(port);
		final Listener listener = (Listener) services.get(key);
		if (listener == null)
			return;
		listener.pleaseStop();
		services.remove(key);
		log("Stopping service " + listener.service.getClass().getName() +
				" on port " + port);
	}

	/**
	 * nested class to handle listening
	 */
	public class
	Listener
		extends Thread {
		ServerSocket listen_socket;
		int port;
		Service service;
		volatile boolean stop = false;

		/**
		 * the 
		 */
		public
		Listener(ThreadGroup group, int port, Service service)
			throws IOException
		{
			super(group, "Listener:" + port);
			listen_socket = new ServerSocket(port);
			// give it a non-zero timeout so accept() can be interruptied
			listen_socket.setSoTimeout(5000);
			this.port = port;
			this.service = service;
		}

		public void
		pleaseStop() {
			this.stop = true;
			this.interrupt();
			try {
				listen_socket.close();
			}
			catch (IOException e) {
			}
		}

		public void
		run() {
			while (!stop) {
				try {
					Socket client = listen_socket.accept();
					addConnection(client, service);
				}
				catch (InterruptedIOException e) {
				}
				catch (IOException e) {
					log(e);
				}
			}
		}
	}

		protected synchronized void
		addConnection(Socket s, Service service) {
			if (connections.size() >= maxConnections) {
				try {
					PrintWriter out = new PrintWriter(s.getOutputStream());
					out.print("Connection refused; " +
						"the server is busy; please try again later\r\n");
					out.flush();
					// close the connection to the rejected client
					out.close();
					// and log it
					log("connection refused to " +
							s.getInetAddress().getHostAddress() +
							":" + s.getPort() + ": max connections reached.");
				}
				catch (IOException e) {
					log(e);
				}
			} else {
				// create a connection thread to handle the connection
				Connection c = new Connection(s, service);
				// add it to the list of current connections
				connections.add(c);
				// Log this new connection
				log("Connected to " + s.getInetAddress().getHostAddress() +
					":" + s.getPort() + " on port " + s.getLocalPort() +
					" for service " + service.getClass().getName());
				// start the connection to provide the service
				c.start();
			}
		}

		/**
		 * A connection thread calls this method just before it exits. It removes
		 * the specified Connection from the set of connections.
		 */
		protected synchronized void
		endConnection(Connection c) {
			connections.remove(c);
			log("Connection to " + c.client.getInetAddress().getHostAddress() +
				":" + c.client.getPort() + " closed.");
		}

		public synchronized void
		setMaxConnections(int max) {
			maxConnections = max;
		}

	/**
	 * displays status information about the server on the specified stream
	 */
	public synchronized void
	displayStatus(PrintWriter out) {
/*
		Iterator keys = services.keySet().iterator();
		while (keys.hasNext()) {
			Integer port = (Integer)keys.next();
			Listener listener = (Listener)services.get(port);
			out.print("SERVICE " + listener.service.getClass().getName()
				+ " ON PORT " + port + "\r\n");
		}
*/

		// dipslay the current connection limit
    out.print("port:  " + httpPort + "\r\n");
		out.print("MAX CONNECTIONS: " + maxConnections + "\r\n");

		// display a list of all current connections
		Iterator conns = connections.iterator();
		while (conns.hasNext()) {
			Connection c = (Connection)conns.next();
			out.print("CONNECTED TO " + 
						c.client.getInetAddress().getHostAddress() +
						":" + c.client.getPort() + " ON PORT " +
						c.client.getLocalPort() + " FOR SERVICE " +
						c.service.getClass().getName() + "\r\n");
		}
	}

  public void handleCmd(String cmd, String line, PrintWriter out) {

    if (cmd.equalsIgnoreCase("status")) {
      out.print("TelemetryWatcher:\r\n");
      out.print("refreshCount: " + refreshCount + "\r\n");

      displayStatus(out);
    }
    else if (cmd.equalsIgnoreCase("help")) {
      out.print("TelemetryWatcher:\r\n");
      out.print("refreshCount: <secs>\r\n");

      displayStatus(out);
    }
    else if (cmd.equalsIgnoreCase("ports")) {
      out.print("TelemetryWatcher " + httpPort + "\r\n");

    }
    else if (cmd.equalsIgnoreCase("shutdown")) {
      out.print("TelemetryWatcher shutting down\r\n");
      doShutdown = true;
    }
    else if (cmd.equalsIgnoreCase("refreshCount")) {
      StringTokenizer tokens = new StringTokenizer(line);
      // get past the actual command
      String s = tokens.nextToken();
      s = tokens.nextToken();
      int i = Integer.parseInt(s);
      if (i > 0)
        refreshCount = i;
      if (StressManager.getTelemetryServer() != null)
        StressManager.getTelemetryServer().sendTelemetry("TelemetryWatcher.refreshCount", Integer.toString(refreshCount));
      System.out.println("TelemetryWatcher:  refreshCount set to " + refreshCount);
    }
  }

	/**
	 * class that handles an individual connection between a client and a service
	 */
	public class Connection extends Thread {
		Socket client;
		Service service;

		public Connection(Socket client, Service service) {
			super("Server.Connection:" + 
					client.getInetAddress().getHostAddress() +
					":" + client.getPort());
			this.client = client;
			this.service = service;
		}

		/**
		 * the body of each connection thread
		 */
		public void run() {
			try {
				InputStream in = client.getInputStream();
				OutputStream out = client.getOutputStream();
				service.serve(in, out);
			}
			catch (IOException e) {
				log(e);
			}
			finally {
				endConnection(this);
			}
		}

	}

	public interface
	Service {
		public void serve(InputStream in, OutputStream out) throws IOException;
	}

	public static class
	Time
		implements Service {
		public void
		serve(InputStream i, OutputStream o)
			throws IOException {
			PrintWriter out = new PrintWriter(o);
			out.print(new Date() + "\r\n");
			out.close();
			i.close();
		}

	}

  /**
   * Implement our version of a server to pass back the current information
   */
  void initHttpServer()
      throws Exception {
    httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
    httpServer.createContext("/", new DefaultHandler());
    httpServer.createContext("/page", new PageHandler());
    httpServer.createContext("/TelemetryItem", new TelemetryItemHandler());
    httpServer.createContext("/TelemetryList", new TelemetryListHandler());
    httpServer.createContext("/StressManager", new StressManagerHandler());
    // httpServer.setExecutor(Executors.newCachedThreadPool());
    httpServer.start();
    System.out.println("HTTP server running on port " + httpPort);
  }

  class DefaultHandler
    implements HttpHandler {

    public void handle(HttpExchange httpExchange)
        throws IOException {
      StringBuilder response = new StringBuilder();
      Map<String, String> params = queryToMap(httpExchange.getRequestURI().getQuery());
      response.append("<html>");
      response.append("<title>I See You Watching Me</title>");
      response.append("<body>");

      response.append("<h1><center>WidowMaker Telemetry Watcher</center></h1>");
      response.append("<p>");
      response.append("Pages available:");
      response.append("<br>");
      response.append("<ul>");
      response.append("<li><a href=\"/TelemetryList\">List of Active Threads</a>");
      response.append("<li><a href=\"/StressManager\">State of WidowMaker Stress Manager</a>");
      response.append("</ul>");
      response.append("</p>");
      

      response.append("</body>");
      response.append("</html>");

      httpExchange.sendResponseHeaders(200, response.toString().getBytes().length);
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(response.toString().getBytes());
      responseBody.close();
    }
  }

  class PageHandler
    implements HttpHandler {

    private String readFileToString(File f) {

      if (f == null)
        return null;

      StringBuilder builder = new StringBuilder();
      BufferedReader reader = null;

      try {
        reader = new BufferedReader(new FileReader(f));
        char[] buf = new char[1024];
        int r = 0;

        while ((r = reader.read(buf)) != -1) {
          builder.append(buf, 0, r);
        }
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
        reader.close();
        }
        catch (IOException e) {
          // who cares?
          ;
        }
      }

      return builder.toString();
    }

    public void handle(HttpExchange httpExchange)
        throws IOException {

      // System.out.println("Page handle()");

      StringBuilder response = new StringBuilder();
      Map<String, String> params = queryToMap(httpExchange.getRequestURI().getQuery());

      response.append("<html>");
      response.append("<body>");
      response.append("\n");

      String pageToRender = params.get("name");
      boolean pageExists = true;
      File pageFile = null;

      response.append("<p>Page being requested:  " + pageToRender + "</p>");
      response.append("\n");

      // System.out.println("page being requested:  " + pageToRender);

      pageFile = new File(pageToRender);
      pageExists = pageFile.exists();

      String pageContents = null;

      if (pageExists) {
        response.append("<p>The page exists</p>");
        pageContents = readFileToString(pageFile);
      } else {
        response.append("<p>The page DOES NOT exist</p>");
      }

      response.append("\n");

      response.append("</body>");
      response.append("</html>");

      // substitutions
      // __REFRESH_COUNT__
      // __TELEMETRY_WATCHER_HOST__
      // __TELEMETRY_WATCHER_PORT__
      // __HOSTNAME__

      if (pageContents != null) {
        pageContents = pageContents.replaceAll("__REFRESH_COUNT__",
                          Integer.toString(refreshCount));
        pageContents = pageContents.replaceAll("__TELEMETRY_WATCHER_HOST__",
                          localHostName);
        pageContents = pageContents.replaceAll("__TELEMETRY_WATCHER_PORT__",
                          Integer.toString(httpPort));
        pageContents = pageContents.replaceAll("__HOSTNAME__",
                          localHostName);
        response = new StringBuilder();
        response.append(pageContents);
      }

      httpExchange.sendResponseHeaders(200, response.toString().getBytes().length);
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(response.toString().getBytes());
      responseBody.close();
    }
  }

  class TelemetryItemHandler
    implements HttpHandler {

    public void handle(HttpExchange httpExchange)
        throws IOException {
      StringBuilder response = new StringBuilder();
      Map<String, String> params = queryToMap(httpExchange.getRequestURI().getQuery());

      String telemetryItem = params.get("item");

      if (telemetryItem != null) {
        String val = telemetryData.get(telemetryItem);
        if (val != null) {
          Headers headers = httpExchange.getResponseHeaders();
          headers.set("Content-Type", "text/plain");
          headers.set("TelemetryItem", val);
          response.append(val);
        } else {
          response.append("unknown " + telemetryItem);
        }
      } else {
        response.append("no item provided");
      }

      httpExchange.sendResponseHeaders(200, 0);
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(response.toString().getBytes());
      responseBody.close();
    }
  }

  class StressManagerHandler
    implements HttpHandler {

    public void handle(HttpExchange httpExchange)
        throws IOException {
      StringBuilder response = new StringBuilder();
      Map<String, String> params = queryToMap(httpExchange.getRequestURI().getQuery());

      // System.out.println("StressManagerHandler");

      String item = params.get("item");
      if (item != null) {
        if (item.equalsIgnoreCase("current_time")) {
          handleCurrentTime(httpExchange);
          return;
        }
        else if (item.equalsIgnoreCase("current_datetime")) {
          handleCurrentDateTime(httpExchange);
          return;
        }
      }

      response.append("<html>");
      response.append("\n");
      response.append("<head>");
      response.append("\n");
      response.append("<meta http-equiv=\"refresh\" content=\"" + refreshCount + "\">");
      response.append("\n");
      response.append("<title>");
      response.append("\n");
      response.append("WidowMaker Stress Manager Status Page");
      response.append("\n");
      response.append("</title>");
      response.append("\n");
      response.append("</head>");
      response.append("\n");
      response.append("<body>");
      response.append("\n");
      response.append("<center><h1>WidowMaker Stress Manager Page</h1></center>");
      response.append("\n");
      response.append("<p>\n");
      response.append("<strong>host:</strong>  " + localHostName);
      response.append("\n");
      response.append("</p>\n");
      response.append("<div class='header'><strong>Stress Manager Stats</strong></div>");
      response.append("\n");
      response.append("<table>");
      response.append("\n");
      response.append("<tr>");
      response.append("\n");
      response.append("<td width=300><strong>Name</strong></td>");
      response.append("\n");
      response.append("<td width=300><strong>Value</strong></td>");
      response.append("\n");
      response.append("</tr>");
      response.append("\n");
      response.append("</table>");
      response.append("\n");
      response.append("<table>");
      response.append("\n");

      response.append("<tr>");
      response.append("\n");
      response.append("<td width=300>Current Time</td>");
      response.append("\n");
      response.append("<td width=300>");
      response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:350px; height:25px;' src='http://" + localHostName + ":" + httpPort + "/StressManager?item=current_time'>");
      response.append("\n");
      response.append("</iframe>");
      response.append("</td>");
      response.append("\n");
      response.append("</tr>");
      response.append("\n");

      response.append("<tr>");
      response.append("\n");
      response.append("<td width=300>Current DateTime</td>");
      response.append("\n");
      response.append("<td width=300>");
      response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:350px; height:25px;' src='http://" + localHostName + ":" + httpPort + "/StressManager?item=current_datetime'>");
      response.append("\n");
      response.append("</iframe>");
      response.append("</td>");
      response.append("\n");
      response.append("</tr>");
      response.append("\n");

      response.append("<tr>");
      response.append("\n");
      response.append("<td width=300>Min Users</td>");
      response.append("\n");
      response.append("<td width=300>");
        response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:350px; height:25px;' src='http://" + localHostName + ":" + httpPort + "/TelemetryItem?item=StressManager.minusers'>");
        response.append("\n");
        response.append("</iframe>");
      response.append("</td>");
        response.append("\n");
      response.append("</tr>");
      response.append("\n");

      response.append("<tr>");
      response.append("\n");
      response.append("<td width=300>Max Users</td>");
      response.append("\n");
      response.append("<td width=300>");
        response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:350px; height:25px;' src='http://" + localHostName + ":" + httpPort + "/TelemetryItem?item=StressManager.maxusers'>");
        response.append("\n");
        response.append("</iframe>");
      response.append("</td>");
        response.append("\n");
      response.append("</tr>");
      response.append("\n");

      response.append("<tr>");
      response.append("\n");
      response.append("<td width=300>Currently Running</td>");
      response.append("\n");
      response.append("<td width=300>");
        response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:350px; height:25px;' src='http://" + localHostName + ":" + httpPort + "/TelemetryItem?item=StressManager.current_running'>");
        response.append("\n");
        response.append("</iframe>");
      response.append("</td>");
        response.append("\n");
      response.append("</tr>");
      response.append("\n");

      response.append("<tr>");
      response.append("\n");
      response.append("<td width=300>Next Test to Start</td>");
      response.append("\n");

      response.append("<td width=300>");
        response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:350px; height:25px;' src='http://" + localHostName + ":" + httpPort + "/TelemetryItem?item=StressManager.nextTestToStart'>");
        response.append("\n");
        response.append("</iframe>");
      response.append("</td>");
        response.append("\n");
      response.append("</tr>");
      response.append("\n");

      response.append("<tr>");
      response.append("<td width=300>Refresh Thread</td>");
      response.append("\n");

      response.append("<td width=300>");
        response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:350px; height:25px;' src='http://" + localHostName + ":" + httpPort + "/TelemetryItem?item=refreshThread.start'>");
        response.append("\n");
        response.append("</iframe>");
      response.append("</td>");
        response.append("\n");
      response.append("</tr>");
      response.append("\n");

      response.append("</table>");
      response.append("\n");
      response.append("<div id=\"variable_row\">");
      response.append("\n");

      response.append("</body>");
      response.append("\n");
      response.append("</html>");
      response.append("\n");

      httpExchange.sendResponseHeaders(200, response.toString().getBytes().length);
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(response.toString().getBytes());
      responseBody.close();
    }

    public void handleCurrentDateTime(HttpExchange httpExchange)
      throws IOException {
      StringBuilder response = new StringBuilder();

      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

      String val = formatter.format(new Date());

      Headers headers = httpExchange.getResponseHeaders();
      headers.set("Content-Type", "text/plain");
      headers.set("TelemetryItem", val);
      response.append(val);

      httpExchange.sendResponseHeaders(200, 0);
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(response.toString().getBytes());
      responseBody.close();
    }

    public void handleCurrentTime(HttpExchange httpExchange)
      throws IOException {
      StringBuilder response = new StringBuilder();

      String val = new Date().toString();

      Headers headers = httpExchange.getResponseHeaders();
      headers.set("Content-Type", "text/plain");
      headers.set("TelemetryItem", val);
      response.append(val);

      httpExchange.sendResponseHeaders(200, 0);
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(response.toString().getBytes());
      responseBody.close();
    }
  }

  class TelemetryListHandler
    implements HttpHandler {

    public void handle(HttpExchange httpExchange)
        throws IOException {
      StringBuilder response = new StringBuilder();
      Map<String, String> params = queryToMap(httpExchange.getRequestURI().getQuery());

      String currentTime = new Date().toString();

      response.append("<html>");
      response.append("\n");
      response.append("<head>");
      response.append("\n");
      response.append("<meta http-equiv=\"refresh\" content=\"" + refreshCount + "\">");
      response.append("\n");
      response.append("<title>");
      response.append("\n");
      response.append("WidowMaker Thread Status Page");
      response.append("\n");
      response.append("</title>");
      response.append("\n");
      response.append("</head>");
      response.append("\n");
      response.append("<body>");
      response.append("\n");
      response.append("<center><h1>WidowMaker Thread Status Page</h1></center>");
      response.append("\n");
      response.append("<p>");
      response.append("\n");
      response.append("<div class='header'>System Info</div>");
      response.append("\n");
      response.append("<table>");
      response.append("\n");
      response.append("<tr>");
      response.append("\n");
      response.append("<td>");
      response.append("\n");
      response.append("host:");
      response.append("\n");
      response.append("</td>");
      response.append("\n");
      response.append("<td>");
      response.append("\n");
      response.append("<strong>" + localHostName + "</strong>");
      response.append("\n");
      response.append("</td>");
      response.append("\n");
      response.append("</tr>");
      response.append("\n");
      response.append("<tr>");
      response.append("\n");
      response.append("<td>");
      response.append("\n");
      response.append("Current time:");
      response.append("\n");
      response.append("</td>");
      response.append("\n");
      response.append("<td>");
      response.append("\n");
      response.append("<strong>" + currentTime + "</strong>");
      response.append("\n");
      response.append("</td>");
      response.append("\n");
      response.append("</tr>");
      response.append("\n");
      response.append("</tr>");
      response.append("\n");
      response.append("</table>");
      response.append("\n");
      response.append("</p>");
      response.append("\n");
      response.append("<div class='header'>Status Per Thread</div>");
      response.append("\n");
      response.append("<table>");
      response.append("\n");
      response.append("<tr>");
      response.append("\n");
      response.append("<td width=200>Thread Number</td>");
      response.append("\n");
      response.append("<td width=300>Thread Name</td>");
      response.append("\n");
      response.append("<td width=300>Test Name</td>");
      response.append("\n");
      response.append("<td width=100>Count</td>");
      response.append("\n");
      response.append("<td width=300>Start Time</td>");
      response.append("\n");
      response.append("</tr>");
      response.append("\n");
      response.append("<div id=\"variable_row\">");
      response.append("\n");

      int highThread = -1;
      Enumeration enumerator = telemetryData.keys();
      while (enumerator.hasMoreElements()) {
        String key = (String)enumerator.nextElement();
        if (key.startsWith("testthread.")) {
          String[] parts = key.split("\\.");
          if (parts.length > 2) {
            int num = Integer.parseInt(parts[1]);
            if (num > highThread)
              highThread = num;
          }
        }
      }

      // System.out.println("high thread is " + highThread);

      int ii;
      for ( ii = 0; ii <= highThread; ii++) {

        // System.out.println("processing thread " + ii);

        // response.append("<div id=\"variable_label\">Thread " + ii + ":</div>");
        // response.append("\n");
        response.append("<div id=\"variable_value\">");
        response.append("\n");
        response.append("<tr>");
        response.append("\n");
        response.append("<td width=200>");
        response.append("\n");
        response.append("Thread " + ii + ":");
        response.append("\n");
        response.append("</td>");
        response.append("\n");
        response.append("<td>");
        response.append("\n");
        response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:300px; height:22px;' src='http://" + localHostName + ":" + httpPort + "/TelemetryItem?item=testthread." + ii + ".testname'>");
        response.append("\n");
        response.append("</iframe>");
        response.append("\n");
        response.append("</td>");
        response.append("\n");

        response.append("<td>");
        response.append("\n");
        response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:300px; height:22px;' src='http://" + localHostName + ":" + httpPort + "/TelemetryItem?item=testthread." + ii + ".testfile'>");
        response.append("\n");
        response.append("</iframe>");
        response.append("\n");
        response.append("</td>");
        response.append("\n");

        response.append("<td>");
        response.append("\n");
        response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:100px; height:22px;' src='http://" + localHostName + ":" + httpPort + "/TelemetryItem?item=testthread." + ii + ".counter'>");
        response.append("\n");
        response.append("</iframe>");
        response.append("\n");
        response.append("</td>");
        response.append("\n");
        response.append("<td>");
        response.append("\n");
        response.append("<iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:300px; height:22px;' src='http://" + localHostName + ":" + httpPort + "/TelemetryItem?item=testthread." + ii + ".starttime'>");
        response.append("\n");
        response.append("</iframe>");
        response.append("\n");
        response.append("</td>");
        response.append("\n");
        response.append("</tr>");
        response.append("\n");
        response.append("</div>");
        response.append("\n");
        response.append("</div>");
        response.append("\n");
      }





/**
 *
<div id="variable_label">Thread #1:</div>
<div id="variable_value">
 <iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:100px; height:75px;' src='HOST:PORT/TelemetryItem/item=THREAD_NAME'>
 </iframe>
 <iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:100px; height:75px;' src='HOST:PORT/TelemetryItem/item=THREAD_COUNT'>
 </iframe>
 <iframe frameborder='0' scrolling='no' style='padding: 0px; margin: 0px; width:100px; height:75px;' src='HOST:PORT/TelemetryItem/item=THREAD_START'>
 </iframe>
 </div>
</div>
*/

      response.append("</table>");
      response.append("\n");


      response.append("</body>");
      response.append("\n");
      response.append("</html>");
      response.append("\n");

      httpExchange.sendResponseHeaders(200, 0);
      OutputStream responseBody = httpExchange.getResponseBody();
      responseBody.write(response.toString().getBytes());
      responseBody.close();
    }
  }

  public static Map<String, String> queryToMap(String query) {
    Map<String, String> result = new HashMap<String, String>();

      if (query == null) {
        // System.out.println("queryToMap: query is null");
        return result;
      }
      if (query.length() == 0) {
        // System.out.println("queryToMap: query query length is 0");
        return result;
      }
    for ( String param : query.split("&")) {
      String pair[] = param.split("=");
      if (pair.length > 1) {
        result.put(pair[0], pair[1]);
      } else {
        result.put(pair[0], "");
      }
    }
    return result;
  }


	public static void main(String[] args) {
		try {
      /*
			if (args.length < 2)
				throw new IllegalArgumentException("Must specify a service");
      */

			TelemetryWatcher s = new TelemetryWatcher(Logger.getLogger(TelemetryWatcher.class.getName()),
				Level.INFO, 10);

			int i = 0;
			while (i < args.length) {
				if (args[i].equals("-control")) {
					++i;
					String password = args[i++];
					int port = Integer.parseInt(args[i++]);
					// s.addService(new Control(s, password), port);
				} else {
					// start a named service on the speicified port
					// dynamically loadn and instantiate a Service class
					String serviceName = args[i++];
					Class serviceClass = Class.forName(serviceName);
					Service service = (Service)serviceClass.newInstance();
					int port = Integer.parseInt(args[i++]);
					// s.addService(service, port);
				}
			}

      s.initialize();

		}
		catch (Exception e) {
			System.err.println("server:  " + e);
			System.err.println("Usage:  java Server " +
								"[-control <password> <port>] " +
								"[<servicename> <port> ... ]");
			System.exit(1);
		}
	}

}


