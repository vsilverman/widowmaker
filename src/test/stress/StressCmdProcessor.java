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


package test.stress;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;


public class
StressCmdProcessor
{

  public static final int DEFAULT_PORT = 26666;

  protected int port;
  protected int maxConnections = 10;
  boolean isInitialized = false;
  protected Set connections;
  protected ArrayList<StressCmdHandler> handlers;
  protected ThreadGroup threadGroup;
  Listener listener = null;
  Control cmdController = null;

  private static StressCmdProcessor thisServer;

  public StressCmdProcessor() {
    handlers = new ArrayList<StressCmdHandler>();
    threadGroup = new ThreadGroup("ServerCmdProcessor");
    cmdController = new Control(this);
    connections = new HashSet(maxConnections);
    port = 0;
  }

  public StressCmdProcessor(int port)
    throws IOException
  {
    this();
    this.port = port;
  }

  public static synchronized StressCmdProcessor
  getStressCmdProcessor() {
    if (thisServer == null) {
      try {
        thisServer = new StressCmdProcessor(DEFAULT_PORT);
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }
    return thisServer;
  }

  // TODO: what if we're already connected? future work
  public void setPort(int port)
  {
    if (!isInitialized)
      this.port = port;
  }

  public synchronized void initialize()
    throws IOException
  {
    if (isInitialized)
      return;

    // make sure this isn't called twice
    if (listener == null) {
      listener = new Listener(threadGroup, port, cmdController);
      listener.start();
    }

    isInitialized = true;
  }

  public void addHandler(StressCmdHandler handler) {
    if (handler == null)
      return;

    log("adding handler");
    handlers.add(handler);
  }

  public void removeHandler(StressCmdHandler handler) {

    if (handler == null)
      return;

    log("removing handler");
    handlers.remove(handler);
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
		StressCmdProcessor server;
		String password;
		boolean connected = false;

		/**
		 * create a new Control service.
		 */
		public
		Control(StressCmdProcessor server) {
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

			for (;;) {
				out.print("> ");
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
            // get list of handlers
            // pass the command to each handler
            Iterator iter = handlers.iterator();
            while (iter.hasNext()) {
              StressCmdHandler handler = (StressCmdHandler)iter.next();
              handler.handleCmd(command, line, out);
            }
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
   * testing hook
   */
	public static void main(String[] args) {
		try {
			if (args.length < 2)
				throw new IllegalArgumentException("Must specify a service");

			StressCmdProcessor s = null;

			int i = 0;
			while (i < args.length) {
				if (args[i].equals("-control")) {
					++i;
					int port = Integer.parseInt(args[i++]);
          s = new StressCmdProcessor(port);
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

