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



package test.utilities;

import java.util.ArrayList;
import java.util.Iterator;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;
import com.marklogic.semantics.jena.client.JenaDatabaseClient;
import com.marklogic.semantics.jena.engine.MarkLogicQueryEngine;
import com.marklogic.semantics.jena.engine.MarkLogicUpdateEngine;

/*

I need a MarkLogicDatasetGraph

to make that, I need a DatabaseClient, which I make from the factory. I need host, port, etc for this

with that DatabaseClient, I can make a JenaDatabaseClient, and pass into it the DatabaseClient

*/

public class MarkLogicWrapperFactory {

  private static ArrayList<MarkLogicWrapper> wrappers = new ArrayList<MarkLogicWrapper>();

/*
    static MarkLogicWrapper createWrapper(DatabaseClient client) {
        JenaDatabaseClient jenaClient = new JenaDatabaseClient(client);
        MarkLogicWrapper wrapper = new MarkLogicWrapper(jenaClient);
        MarkLogicQueryEngine.unregister();
        MarkLogicQueryEngine.register();
        MarkLogicUpdateEngine.unregister();
        MarkLogicUpdateEngine.register();

        return wrapper;
    }
*/

    static public MarkLogicWrapper createWrapper(String host,
                                                    int port,
                                                    String user,
                                                    String password,
                                                    Authentication type) {

        MarkLogicWrapper wrapper = null;

        Authentication atype;
        if (type == null)
            atype = Authentication.DIGEST;
        else
            atype = type;

        // System.out.println("fetching DatabaseClient");
        // System.out.println("host:  " + host);
        // System.out.println("port:  " + port);
        // System.out.println("user:  " + user);
        // System.out.println("password:  " + password);

        try {
        DatabaseClient databaseClient =
                    DatabaseClientFactory.newClient(host, port, user, password, atype);

        JenaDatabaseClient jenaClient = new JenaDatabaseClient(databaseClient);
        MarkLogicDatasetGraph mlDatasetGraph = new MarkLogicDatasetGraph(jenaClient);
        wrapper = new MarkLogicWrapper(mlDatasetGraph, databaseClient, jenaClient);

        wrapper.setHost(host);
        wrapper.setPort(port);
        wrapper.setUser(user);
        wrapper.setPassword(password);
        wrapper.setAuthenticationType(atype);

        wrapper.initialize();
        } catch (Throwable t) {
          t.printStackTrace();
        }
        return wrapper;
    }

    static synchronized public MarkLogicWrapper getWrapper(String host,
                                                    int port,
                                                    String user,
                                                    String password,
                                                    Authentication type) {

      MarkLogicWrapper wrapper;
      Iterator iter = wrappers.iterator();
      while (iter.hasNext()) {
        wrapper = (MarkLogicWrapper)iter.next();
        if ((host.equals(wrapper.host)) && (port == wrapper.port))
          return wrapper;
      }

      wrapper = createWrapper(host, port, user, password, type);
      wrappers.add(wrapper);

      return wrapper;
    }

}

