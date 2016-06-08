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

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;
import com.marklogic.semantics.jena.client.JenaDatabaseClient;
import com.marklogic.semantics.jena.engine.MarkLogicQueryEngine;
import com.marklogic.semantics.jena.engine.MarkLogicUpdateEngine;
import com.marklogic.semantics.jena.MarkLogicJenaException;


import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.DocumentMetadataHandle;;

import com.marklogic.client.admin.ServerConfigurationManager;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;





public class MarkLogicWrapper
{
    protected String                host;
    protected int                   port;
    protected String                user;
    protected String                password;
    protected DatabaseClient        client;
    protected Authentication        authenticationType;
    protected MarkLogicDatasetGraph datasetGraph;
    protected DatabaseClient        databaseClient;
    protected JenaDatabaseClient    jenaClient;

    MarkLogicWrapper() {
    }

    MarkLogicWrapper(String host,
                                int port,
                                String user,
                                String password,
                                Authentication type) {

        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.authenticationType = type;
    }

    MarkLogicWrapper(MarkLogicDatasetGraph dsg,
                        DatabaseClient databaseClient,
                        JenaDatabaseClient jenaClient) {

        this.datasetGraph = dsg;
        this.databaseClient = databaseClient;
        this.jenaClient = jenaClient;

    }

    public void     setHost(String host) {
        this.host = host;
    }

    public String   getHost() {
        return host;
    }

    public void     setPort(int port) {
        this.port = port;
    }

    public int      getPort() {
        return port;
    }

    public void     setUser(String user) {
        this.user = user;
    }

    public String   getUser() {
        return user;
    }

    public void     setPassword(String password) {
        this.password = password;
    }

    public String   getPassword() {
        return password;
    }

    public void     setAuthenticationType(Authentication type) {
        this.authenticationType = type;
    }

    public void initialize() {
        connect();
        MarkLogicQueryEngine.unregister();
        MarkLogicQueryEngine.register();
        MarkLogicUpdateEngine.unregister();
        MarkLogicUpdateEngine.register();

        // TODO:  temporary, needs to be passed in
        setServerRequestLogging(true);

    }

    public DatabaseClient connect() {
        if (client == null) {
            client = DatabaseClientFactory.newClient(host, port, user, password,
                            Authentication.DIGEST);
        }
        return client;
    }

    public void setServerRequestLogging(boolean bval) {

      connect();
      ServerConfigurationManager manager = client.newServerConfigManager();
      manager.readConfiguration();
      if (manager.getServerRequestLogging() != bval) {
        manager.setServerRequestLogging(bval);
        manager.writeConfiguration();
      }
    }

    public void flush() {
      if (datasetGraph != null) {
        // System.out.println("flushing datasetGraph");
        datasetGraph.sync();
      }
    }

    public void disconnect() {
        client = null;
    }

    public boolean isConnected() {
        return (client != null);
    }

    public void shutdown() {

        datasetGraph.sync();
        try {
            datasetGraph.close();
        } catch (MarkLogicJenaException e) {
            System.err.println("ignoring MarkLogicJenaException during close");
        }

        if (client != null) {
            disconnect();
        }
    }

    public MarkLogicDatasetGraph getDatasetGraph() {
        if (client == null)
            connect();
        if (datasetGraph == null) {
            datasetGraph = new MarkLogicDatasetGraph(jenaClient);
        }
        return datasetGraph;
    }

    public void insertXmlDocument(String filename, String docId)
        throws IOException {

        InputStream docStream = null;
        try {
            docStream = new FileInputStream(filename);
        } catch (FileNotFoundException e) {
            System.err.println("file not found:  " + filename);
            throw e;
        }

        insertXmlDocument(docStream, docId);
    }

    public void insertXmlDocument(InputStream instream, String docId)
        throws IOException {

        XMLDocumentManager docMgr = client.newXMLDocumentManager();

        InputStreamHandle handle = new InputStreamHandle();
        handle.set(instream);

        docMgr.write(docId, handle);

    }

    public void insertXmlDocument(String filename, String docId, String collectionName)
        throws IOException {

        InputStream docStream = null;
        try {
            docStream = new FileInputStream(filename);
        } catch (FileNotFoundException e) {
            System.err.println("file not found:  " + filename);
            throw e;
        }

        insertXmlDocument(docStream, docId, collectionName);
    }

    public void insertXmlDocument(InputStream instream, String docId, String collectionName)
        throws IOException {

        XMLDocumentManager docMgr = client.newXMLDocumentManager();

        InputStreamHandle handle = new InputStreamHandle();
        handle.set(instream);

        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        metadata.getCollections().addAll(collectionName);

        docMgr.write(docId, metadata, handle);

    }
}


