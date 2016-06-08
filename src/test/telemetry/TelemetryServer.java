/*
 * Copyright 2003-2013 MarkLogic Corporation
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


package test.telemetry;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

import test.utilities.StressCmdHandler;

public class
TelemetryServer
  implements StressCmdHandler
{

  protected int port;
  boolean isInitialized = false;
  protected int maxConnections = 10;
  protected Set connections;
  protected ArrayList<StressCmdHandler> handlers;
  protected ThreadGroup threadGroup;
  Listener listener = null;
  Control cmdController = null;
  protected Hashtable<String, String> telemetryData;

  private static TelemetryServer thisServer;
  public static final int DEFAULT_TELEMETRY_PORT = 26667;
  private RefreshThread refreshThread = null;

  public TelemetryServer() {
    System.out.println("TelemetryServer constructor");
    handlers = new ArrayList<StressCmdHandler>();
    threadGroup = new ThreadGroup("TelemetryServer");
    cmdController = new Control(this);
    connections = new HashSet(maxConnections);
    telemetryData = new Hashtable<String, String>();
    port = DEFAULT_TELEMETRY_PORT;
  }

  public TelemetryServer(int port)
    throws IOException
  {
    this();
    System.out.println("TelemetryServer(port) constructor");
    this.port = port;
  }

  /**
   * eventually this will log somewhere specified
   */
  public void
  log(String s) {
    System.out.println(s);
  }
  
  public void
  log(Object o) {
    log(o.toString());
  }

  public static synchronized TelemetryServer
  getTelemetryServer() {

    if (thisServer == null) {
      try {
        thisServer = new TelemetryServer(DEFAULT_TELEMETRY_PORT);
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
    return thisServer;
  }

  /**
 * set the port number for running telemetry
 * this is only valid if we have not already been initialized
 *
 * @port  port number the server is running on
 */
  public void
  setPort(int port) {
    if (!isInitialized)
      this.port = port;
  }

  public synchronized void
  initialize()
    throws IOException {

    if (isInitialized)
      return;

    // empty for now, gives us a chance for the future
    listener = new Listener(threadGroup, port, cmdController);
    listener.start();
    refreshThread = new RefreshThread(this);
    refreshThread.start();

    isInitialized = true;
  }

  public void
  shutdown() {
    // shut down everything
    if (listener != null)
      listener.pleaseStop();
    if (refreshThread != null)
      refreshThread.stopRequest();
  }

  public static final long TELEMETRY_REFRESH_THREAD_DEFAULT_INTERVAL = 180000L;

  public class RefreshThread
    extends Thread {
    // an empty service thread
    volatile boolean stopRequested = false;
    long refreshInterval;
    TelemetryServer parent = null;

    public RefreshThread(TelemetryServer parent) {
      this.parent = parent;
      stopRequested = false;
      refreshInterval = TELEMETRY_REFRESH_THREAD_DEFAULT_INTERVAL;
    }

    public void run() {
			while (!stopRequested) {
				try {
          sleep(refreshInterval);
          // just making sure the timer is getting set right
          if (parent != null) {
            parent.refreshTelemetry();
          }
				}
				catch (InterruptedException e) {
          // do nothing - this just happens
				}
				catch (Exception e) {
					log(e);
				}
			}
		}

    public synchronized void
    setRefreshInterval(long interval) {
      refreshInterval = interval;
    }

    public void stopRequest() {
      stopRequested = true;
      this.notify();
    }
  }

  public void handleCmd(String cmd, String line, PrintWriter out) {

    if (cmd.equalsIgnoreCase("status")) {
      out.print("TelemetryServer\r\n");
      displayStatus(out);
    }
    if (cmd.equalsIgnoreCase("ports")) {
      out.print("TelemetryServer " + port + "\r\n");
    }
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
	 * non-trivial service. command-based protocol.
	 */
	public class Control
		implements Service {
		TelemetryServer server;
		String password;
		boolean connected = false;

		/**
		 * create a new Control service.
		 */
		public
		Control(TelemetryServer server) {
			this.server = server;
		}

		/**
		 * this is the serve method that provides the service.
		 * parses each line to handle the command.
		 */
		public void
		serve(InputStream i, OutputStream o)
			throws IOException {

			BufferedReader in = new BufferedReader(new InputStreamReader(i));
			PrintWriter out = new PrintWriter(o);

			String line;
			boolean authorized = false;

      // TODO:  we don't really do anything here. serious.
      //
			for (;;) {
				// out.print("> ");
				out.flush();
				line = in.readLine();
				if (line == null)		// quit if we get EOF
					break;

				try {
					// use a StringTokenizer to parse the user's command
					StringTokenizer t = new StringTokenizer(line);
					if (!t.hasMoreTokens())
						continue;
					String command = t.nextToken().toLowerCase();

          if (command.equals("internal")) {
          }
          else if (command.equals("time")) {
            out.print(new Date() + "\r\n");
          }
					else if (command.equals("help")) {
						out.print("you should get some!\r\n");
					}
					else if (command.equals("refresh")) {
						out.print("we'll get there!\r\n");
					}
					else if (command.equals("quit")) {
						break;
					}
					else if (command.equals("status")) {
            out.print("control is running\r\n");
					  server.displayStatus(out);
            Iterator iter = handlers.iterator();
            while (iter.hasNext()) {
              StressCmdHandler handler = (StressCmdHandler)iter.next();
              handler.handleCmd(command, line, out);
            }
          } else {
            // get list of handlers
            // pass the command to each handler
            Iterator iter = handlers.iterator();
            while (iter.hasNext()) {
              StressCmdHandler handler = (StressCmdHandler)iter.next();
              handler.handleCmd(command, line, out);
            }
          }


				}
				catch (Exception e) {
					out.print("ERROR WHILE PARSING OR EXECUTING COMMAND:\r\n" +
						e + "\r\n");
          e.printStackTrace();
				}
			}

			// clean up now
			connected = false;
			out.close();
			in.close();
		}
	}


	/**
	 * class that handles an individual connection between a client and a service
	 */
	public class Connection extends Thread {
		Socket client;
		Service service;
    PrintWriter out = null;

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

    public void sendTelemetry(String key, String value) {

      try {
        if (out == null)
          out = new PrintWriter(client.getOutputStream());
        out.print(key + ": " + value + "\r\n");
        out.flush();
      }
      catch (IOException e) {
        // we may need to count the failures and then clean them out if too many
        e.printStackTrace();
      }
    }
	}

	/**
	 * this method makes the server start providing a new service
	 * it runs the specified Service object on the specified port
	 */
/*
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
*/

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
        refreshTelemetry();
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
			out.print("SERVICE " + listener.service.getClass().getName()
				+ " ON PORT " + port + "\r\n");

		// dipslay the current connection limit
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


	/**
	 * displays status information about the server on the specified stream
	 */
	public synchronized void
	sendTelemetry(String key, String value) {

    // update the last known value for the key
    telemetryData.put(key, value);

		// display a list of all current connections
		Iterator conns = connections.iterator();
		while (conns.hasNext()) {
			Connection c = (Connection)conns.next();
      c.sendTelemetry(key, value);
		}
	}

  /**
   * resends all the last known values for each key we've encountered periodically so
   * that a new viewer can get current. 
   * NOTE:  this is strictly the last value encountered for each key. We do not keep history,
   * so this is no opportunity to replay a time series
   */
  public synchronized void
  refreshTelemetry() {

    sendTelemetry("refreshThread.start", new Date().toString());

    Iterator keys = telemetryData.keySet().iterator();
    while (keys.hasNext()) {
      String key = (String)keys.next();
      String value = telemetryData.get(key);
      sendTelemetry(key, value);
    }
    sendTelemetry("refreshThread.stop", new Date().toString());
  }


	/**
	 * displays status information about the server on the specified stream
	 */
	public synchronized void
	shutdownConnections() {

		// display a list of all current connections
		Iterator conns = connections.iterator();
		while (conns.hasNext()) {
			Connection c = (Connection)conns.next();
      c.sendTelemetry("STOP", "shutting down");
      endConnection(c);
		}
	}












	public static void main(String[] args) {
		try {
			if (args.length < 2)
				throw new IllegalArgumentException("Must specify a service");

			TelemetryServer s = null;

			int i = 0;
			while (i < args.length) {
				if (args[i].equals("-control")) {
					++i;
					// String password = args[i++];
					int port = Integer.parseInt(args[i++]);
          s = new TelemetryServer(port);
				}
			}
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

