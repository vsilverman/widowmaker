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



package test.utilities;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.util.Collections;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.semantics.jena.MarkLogicDatasetGraph;
import com.marklogic.semantics.jena.MarkLogicDatasetGraphFactory;
import com.marklogic.semantics.jena.MarkLogicJenaException;

// for sparql update
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;

import org.joda.time.DateTime;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.shared.PrefixMapping;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.graph.Triple;


/**
 * eventually this will be a static class
 */

public class PrefixMapper {

    protected static final String PREFIX_PROPERTY_PREFIX = "prefixes.";

    private boolean debugMode = false;

    protected HashMap<String, String> prefixes;

    private volatile boolean mapHasChanged = true;
    private String  prefixesAsString = null;
    private String  prefixesAsTurtle = null;

    private static PrefixMapper thisMapper;

    private PrefixMapper() {
        prefixes = new HashMap<String, String>();
    }

    public static PrefixMapper getPrefixMapper() {
        if (thisMapper == null) {
            thisMapper = new PrefixMapper();
        }
        return thisMapper;
    }

    public static void setDebugMode(boolean val) {
        thisMapper.debugMode = val;
    }

    /**
     * TODO:  this stuff needs to move into a prefix handler
     */

    public synchronized void addPrefix(String prefix, String iri) {
        prefixes.put(prefix, iri);

        mapHasChanged = true;
    }

    public synchronized void removePrefix(String prefix) {
        if (prefixes.containsKey(prefix))
            prefixes.remove(prefix);

        mapHasChanged = true;
    }

    public Iterator getPrefixes() {

        return prefixes.keySet().iterator();
    }

    public String getNamespace(String prefix) {
      return prefixes.get(prefix);
    }

    private synchronized void calcPrefixesAsString() {
        StringBuilder sb = new StringBuilder();

        Iterator<String> iter = prefixes.keySet().iterator();

        while (iter.hasNext()) {
            String prefix = (String)iter.next();
            sb.append("prefix ");
            sb.append(prefix);
            sb.append(": ");
            String val = prefixes.get(prefix);
            sb.append("<");
            sb.append(val);
            sb.append(">");
            sb.append(" \n");
        }

        prefixesAsString = sb.toString();
    }

    private synchronized void calcPrefixesAsTurtleString() {
        StringBuilder sb = new StringBuilder();

        Iterator<String> iter = prefixes.keySet().iterator();

        while (iter.hasNext()) {
            String prefix = (String)iter.next();
            sb.append("@prefix ");
            sb.append(prefix);
            sb.append(": ");
            String val = prefixes.get(prefix);
            sb.append("<");
            sb.append(val);
            sb.append(">");
            sb.append(" . \n");
        }

        prefixesAsTurtle = sb.toString();
    }

    public String getPrefixesAsString() {

        if (mapHasChanged) {
            calcPrefixesAsString();
            calcPrefixesAsTurtleString();
            mapHasChanged = false;
        }

        return prefixesAsString;
    }

    public String getPrefixesAsTurtleString() {

        if (mapHasChanged) {
            calcPrefixesAsString();
            calcPrefixesAsTurtleString();
            mapHasChanged = false;
        }

        return prefixesAsTurtle;
    }

    public String expandPrefix(String s) {
        if (s == null)
            return null;

        // make a new one so that anything that happened along the way is accounted for
        PrefixMapping prefixMapping = PrefixMapping.Factory.create();
        prefixMapping = prefixMapping.setNsPrefixes(prefixes);
        String str = prefixMapping.expandPrefix(s);
        // System.out.println("expandPrefix:  " + s + ", " + str);
        
        return str;
    }

    public String simplifyWithNamespace(String s) {

        Iterator<String> iter = prefixes.keySet().iterator();

        while (iter.hasNext()) {
            String prefix = (String)iter.next();
            String val = prefixes.get(prefix);
            if (s.startsWith(val)) {
                String rs = s.substring(val.length());
                String nsString = prefix + ":" + rs;
                return nsString;
            }
        }

        return s;
    }

}

