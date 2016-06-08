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

package test.stress;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.Transaction;
import com.marklogic.client.alerting.RuleDefinitionList;
import com.marklogic.client.alerting.RuleManager;
import com.marklogic.client.document.DocumentManager.Metadata;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.extensions.ResourceManager;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.ValuesHandle;
import com.marklogic.client.pojo.PojoPage;
import com.marklogic.client.pojo.PojoRepository;
import com.marklogic.client.pojo.PojoQueryBuilder;
import com.marklogic.client.pojo.PojoQueryDefinition;
import com.marklogic.client.query.AggregateResult;
import com.marklogic.client.query.CountedDistinctValue;
import com.marklogic.client.query.KeyValueQueryDefinition;
import com.marklogic.client.query.RawQueryByExampleDefinition;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.QueryManager.QueryView;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.client.query.ValuesDefinition;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.RDFMimeTypes;
import com.marklogic.client.semantics.SPARQLQueryDefinition;
import com.marklogic.client.semantics.SPARQLQueryManager;
import com.marklogic.client.util.RequestParameters;

import com.fasterxml.jackson.databind.JsonNode;

public class JavaAPISession implements RestSession {
    static private JavaAPISession session;

    private DatabaseClient             client;
    private QueryManager               queryMgr;
    private XMLDocumentManager         xmlDocMgrColl;
    private XMLDocumentManager         xmlDocMgrNoColl;
    private RuleManager                ruleMgr;
    private GenericDocumentManager     genDocMgrColl;
    private GenericDocumentManager     genDocMgrNoColl;
    private GraphManager               graphManager;
    private Map<String,ResourceGetter> getters;
    private PojoRepository<SamplePojo,Long> pojoRepo;
    private PojoQueryBuilder<SamplePojo>    pojoQb;
    private AddDocResource                  addDoc;

    private JavaAPISession(String host, String user, String pass, int port, String database) {
/* Uncomment to debug HTTP client
        System.setProperty(
				"org.apache.commons.logging.simplelog.log.org.apache.http",
				"debug");
 */
/* Alternative to debug HTTP wire exchanges at a lower level
        System.setProperty(
				"org.apache.commons.logging.simplelog.log.org.apache.http.wire",
				"debug");
 */

        if ( database != null && database.length() > 0 ) {
            client = DatabaseClientFactory.newClient(
                host, port, database, user, pass, DatabaseClientFactory.Authentication.DIGEST);
        } else {
            client = DatabaseClientFactory.newClient(
                host, port, user, pass, DatabaseClientFactory.Authentication.DIGEST);
        }

        queryMgr  = client.newQueryManager();
        //queryMgr.setView(QueryView.METADATA);

    	xmlDocMgrColl   = client.newXMLDocumentManager();
    	xmlDocMgrColl.setMetadataCategories(Metadata.COLLECTIONS);

    	xmlDocMgrNoColl = client.newXMLDocumentManager();
    	xmlDocMgrNoColl.clearMetadataCategories();

    	genDocMgrColl = client.newDocumentManager();
    	genDocMgrColl.setMetadataCategories(Metadata.COLLECTIONS);

    	genDocMgrNoColl = client.newDocumentManager();
    	genDocMgrNoColl.clearMetadataCategories();

        graphManager = client.newGraphManager();

    	ruleMgr = client.newRuleManager();

    	pojoRepo = client.newPojoRepository(SamplePojo.class, Long.class);
    	pojoQb = pojoRepo.getQueryBuilder();

    	addDoc = new AddDocResource(client);

    	getters   = new HashMap<String,ResourceGetter>();
    	for (String getter: new String[]{"getDocs", "getElements", "getURIs"}) {
        	getters.put(getter, client.init(getter, new ResourceGetter()));
    	}
    }

    public void message(String msg) {
        System.out.println(StressTester.getTimeString() +
            " (Thread=" + Thread.currentThread().getId() + ") " + msg);
    }

    
    public static synchronized JavaAPISession getRestSession(ConnectionData connData) {
        if (session == null) {
            XCCInfo info = connData.servers.get(0).info;
            session = new JavaAPISession(info.getHost(), info.getUser(), info.getPassword(),
                info.getPort(), info.getDatabase());
        }
        return session;
    }

    @Override
	public Transaction beginTransaction() {
    	return client.openTransaction();
    }

    @Override
	public void commitTransaction(Object transaction) {
    	if (transaction == null) {
    	} else if (transaction instanceof Transaction) {
    		((Transaction) transaction).commit();
    	}
    }

    @Override
	public void rollbackTransaction(Object transaction) {
    	if (transaction == null) {
    	} else if (transaction instanceof Transaction) {
    		((Transaction) transaction).rollback();
    	}
    }

    @Override
    public Map<String, List<String>> makeTransform(
    	String transformName, Map<String, List<String>> transformParams
	) {
    	ServerTransform transform = new ServerTransform(transformName);
    	if (transformParams != null) {
    		transform.putAll(transformParams);
    	}
    	return transform;
    }

    // assumed to be XML
    @Override
    public void putDocument(String uri, byte[] content, Object transaction) {
    	if (transaction instanceof Transaction) {
    		xmlDocMgrNoColl.write(uri, new BytesHandle(content), (Transaction) transaction);
		} else {
        	xmlDocMgrNoColl.write(uri, new BytesHandle(content));
    	}
    }
    @Override
    public void putDocument(String uri, String collection, byte[] content, Object transaction) {
    	if (transaction instanceof Transaction) {
        	if (collection != null) {
        		DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        		metadata.getCollections().add(collection);
        		xmlDocMgrColl.write(uri, metadata, new BytesHandle(content), (Transaction) transaction);
        	} else {
            	xmlDocMgrNoColl.write(uri, new BytesHandle(content), (Transaction) transaction);
        	}
		} else {
        	if (collection != null) {
        		DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        		metadata.getCollections().add(collection);
        		xmlDocMgrColl.write(uri, metadata, new BytesHandle(content));
        	} else {
            	xmlDocMgrNoColl.write(uri, new BytesHandle(content));
        	}
    	}
    }

    @Override
    public void putDocument(String uri, String contentType, String collection, byte[] content, Object transaction) {
    	BytesHandle contentHandle = new BytesHandle(content);
    	if (contentType != null)
    		contentHandle.setMimetype(contentType);

    	if ("application/xml".equals(contentType)) {
    		contentHandle.setFormat(Format.XML);
    	} else if ("application/json".equals(contentType)) {
    		contentHandle.setFormat(Format.JSON);
    	}

    	if (transaction instanceof Transaction) {
        	if (collection != null) {
        		DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        		metadata.getCollections().add(collection);
        		genDocMgrColl.write(uri, metadata, contentHandle, (Transaction) transaction);
        	} else {
        		genDocMgrNoColl.write(uri, contentHandle, (Transaction) transaction);
        	}
		} else {
        	if (collection != null) {
        		DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        		metadata.getCollections().add(collection);
        		genDocMgrColl.write(uri, metadata, contentHandle);
        	} else {
        		genDocMgrNoColl.write(uri, contentHandle);
        	}
    	}
    }

    @Override
    public void putPojo(SamplePojo content, Object transaction, String... collections) {
        pojoRepo.write(content, (Transaction) transaction, collections);
    }

    @Override
	public void putDocument(String uri, byte[] content, Map<String, List<String>> transform, Object transaction) {
    	if (transform instanceof ServerTransform) {
    		if (transaction instanceof Transaction) {
    			xmlDocMgrNoColl.write(
    					uri, new BytesHandle(content), (ServerTransform) transform, (Transaction) transaction
    			);
    		} else {
    			xmlDocMgrNoColl.write(
    					uri, new BytesHandle(content), (ServerTransform) transform
    			);
    		}
    	}
	}

    private void validateBatchPutArgs(List<String> uris, List<String> contentTypes, List<byte[]> contents, List<String> collections,
        Map<String, List<String>> transform, Object transaction)
    {
        if ( uris == null || uris.size() == 0 ) {
            throw new IllegalArgumentException("uris must not be null or empty");
        }
        if ( contentTypes != null && contentTypes.size() != uris.size() ) {
            throw new IllegalArgumentException("contentType.size() must equal uris.size()");
        }
        if ( contents == null || contents.size() != uris.size() ) {
            throw new IllegalArgumentException("contents.size() must equal uris.size()");
        }
        if ( collections != null && collections.size() != uris.size() ) {
            throw new IllegalArgumentException("collections.size() must equal uris.size()");
        }
    }

    @Override
    public void putDocuments(List<String> uris, List<String> contentTypes, List<byte[]> contents,
        List<String> collections, Map<String, List<String>> transform, Object transaction)
    {
        validateBatchPutArgs(uris, contentTypes, contents, collections, transform, transaction);
        GenericDocumentManager docMgr;
        if ( collections == null || collections.size() > 0 ) {
            docMgr = genDocMgrNoColl;
        } else {
            docMgr = genDocMgrColl;
        }
        DocumentWriteSet writeSet = docMgr.newWriteSet();
        for ( int i=0; i < uris.size(); i++ ) {
            String uri         = uris.get(i);
            String contentType = contentTypes == null ? null : contentTypes.get(i);
            byte[] content     = contents.get(i);
            String collection  = collections == null ? null : collections.get(i);

            BytesHandle contentHandle = new BytesHandle(content);
            if (contentType != null) {
                contentHandle.setMimetype(contentType);
            } else {
                // let's assume by default that it's xml
                contentType = "application/xml";
            }
            if ("application/xml".equals(contentType)) {
                contentHandle.setFormat(Format.XML);
            } else if ("application/json".equals(contentType)) {
                contentHandle.setFormat(Format.JSON);
            }
            DocumentMetadataHandle metadataHandle = new DocumentMetadataHandle();
            if ( collection != null ) metadataHandle = metadataHandle.withCollections(collection);
            writeSet.add(uri, metadataHandle, contentHandle);
        }
        docMgr.write(writeSet, (ServerTransform) transform, (Transaction) transaction);
    }

    @Override
    public void putDocumentsViaEval(List<String> uris, List<String> contentTypes, List<byte[]> contents,
        List<String> collections, Object transaction)
    {
        validateBatchPutArgs(uris, contentTypes, contents, collections, null, transaction);
        ServerEvaluationCall call = client.newServerEval();
        // test using XQuery 50% of the time, Javascript the other 50%
        boolean useXQuery = Math.random() >= .5;
        if ( useXQuery ) {
            call.xquery(
                "declare variable $uris external;" +
                "declare variable $contents external;" +
                "declare variable $collections external;" +
                "for $uri at $i in json:array-values($uris) " +
                "return xdmp:document-insert($uri, document {$contents[$i]}, (), $collections[$i])"
            );
        } else {
            call.javascript(
                "declareUpdate();" +
                "var uris, contents, collections;" +
                "for (var i=0; i < uris.length; i++ ) {" +
                "   xdmp.documentInsert(uris[i]," +
                "       new NodeBuilder().addDocument(contents[i]).toNode(), null, collections[i]);" +
                "}");
        }
        call.transaction((Transaction) transaction);

        ArrayNode uriArray = new ObjectMapper().createArrayNode();
        for ( String uri : uris ) uriArray.add(uri);
        call.addVariable("uris", new JacksonHandle().with(uriArray));

        ArrayNode contentArray = new ObjectMapper().createArrayNode();
        for ( byte[] content : contents ) contentArray.add(new String(content));
        call.addVariable("contents", new JacksonHandle().with(contentArray));

        ArrayNode collectionArray = new ObjectMapper().createArrayNode();
        for ( String collection : collections ) collectionArray.add(collection);
        call.addVariable("collections", new JacksonHandle().with(collectionArray));
        call.eval();
    }

    private class AddDocResource extends ResourceManager {
        AddDocResource(DatabaseClient client) {
            super();
            client.init("AddDocResource", this);
        }

        public void putDocument( String uri, String contentType, byte[] content,
            String collection, Transaction transaction) {
            BytesHandle handle = new BytesHandle(content);
            handle.setFormat(Format.valueOf(contentType.toUpperCase()));
            RequestParameters params = new RequestParameters();
            params.put("uri", uri);
            params.put("collection", collection);
            BytesHandle[] input = new BytesHandle[] { handle };
            getServices().put(params, input, transaction, new StringHandle());
        }
    }
    @Override
    public void putDocumentViaExtenstion(String uri, String contentType, byte[] content,
        String collection, Object transaction)
    {
        addDoc.putDocument(uri, contentType, content, collection, (Transaction) transaction);
    }

    @Override
	public void patch(String uri, byte[] change, Object transaction) {
    	if (transaction instanceof Transaction) {
        	xmlDocMgrNoColl.patch(uri, new BytesHandle(change), (Transaction) transaction);
		} else {
        	xmlDocMgrNoColl.patch(uri, new BytesHandle(change));
    	}
	}

    @Override
	public boolean docExists(String uri, Object transaction) {
    	if (transaction instanceof Transaction) {
            return genDocMgrNoColl.exists(uri, (Transaction) transaction) != null;
    	}
        return genDocMgrNoColl.exists(uri) != null;
    }

    @Override
    public String getDocument(String uri, Object transaction) {
        // always XML at present per Norm
    	if (transaction instanceof Transaction) {
        	return xmlDocMgrNoColl.read(uri, new StringHandle(), (Transaction) transaction).get();
    	}
    	return xmlDocMgrNoColl.read(uri, new StringHandle()).get();
    }

    @Override
    public String getDocument(String uri, Map<String, List<String>> transform, Object transaction) {
    	if (transform instanceof ServerTransform) {
        	if (transaction instanceof Transaction) {
        		return xmlDocMgrNoColl.read(
            			uri, new StringHandle(), (ServerTransform) transform, (Transaction) transaction
            			).get();
        	}
    		return xmlDocMgrNoColl.read(
        			uri, new StringHandle(), (ServerTransform) transform
        			).get();
    	}
    	return null;
	}

    @Override
    public long countDocumentsViaBulk(Object transaction, String... uris) {
        if (transaction instanceof Transaction) {
            return genDocMgrNoColl.read((Transaction) transaction, uris).size();
        }
        return genDocMgrNoColl.read(uris).size();
        /*
        StructuredQueryBuilder sqb = client.newQueryManager().newStructuredQueryBuilder();
        QueryDefinition query = sqb.document(uris);
        long start = 1;
        if (transaction instanceof Transaction) {
            return genDocMgrNoColl.search(query, start, (Transaction) transaction).size();
        }
        return genDocMgrNoColl.search(query, start).size();
        */
    }

    @Override
    public long countDocumentsViaPojo(Object transaction, Long... ids) {
        if (transaction instanceof Transaction) {
            return pojoRepo.read(ids, (Transaction) transaction).size();
        }
        return pojoRepo.read(ids).size();
    }

    @Override
    public long countDocumentsViaEval(Object transaction, String... uris) {
        ServerEvaluationCall call = client.newServerEval();
        // test using XQuery 50% of the time, Javascript the other 50%
        boolean useXQuery = Math.random() >= .5;
        if ( useXQuery ) {
            call.xquery(
                "declare variable $uris external;" +
                "fn:count(fn:doc(json:array-values($uris)))");
        } else {
            call.javascript(
                "var uris;" +
                "fn.count(fn.doc(uris))");
        }
        call.transaction((Transaction) transaction);

        ArrayNode uriArray = new ObjectMapper().createArrayNode();
        for ( String uri : uris ) uriArray.add(uri);
        call.addVariable("uris", new JacksonHandle().with(uriArray));

        return call.eval().next().getNumber().longValue();
    }

    @Override
    public long countDocumentsViaExtension(Object transaction, String... uris) {
        String[] params = new String[uris.length * 2];
        for ( int i=0; i < uris.length; i++ ) {
            String uri = uris[i];
            params[i * 2] = "doc";
            params[(i * 2) + 1] = uri;
        }
        String value = getResource("getDocs", "text/plain", params);
        return Long.parseLong(value);
    }

    @Override
    public String getResource(String name, String contentType, String... params) {
    	ResourceGetter getter = getters.get(name);
    	if (getter == null) {
    		throw new IllegalArgumentException("no resource getter named: "+name);
    	}

    	RequestParameters parameters = new RequestParameters();
    	for (int i=0; i < params.length; i++) {
    		parameters.add(params[i], params[++i]);
    	}

    	return getter.get(parameters, contentType);
    }

    @Override
    public int search(String q, String collection, Object transaction) {
    	StringQueryDefinition stringQry = queryMgr.newStringDefinition();
    	if (collection != null)
    		stringQry.setCollections(collection);

    	stringQry.setCriteria(q);

    	if (transaction instanceof Transaction) {
        	return (int) queryMgr.search(stringQry, new SearchHandle(), (Transaction) transaction).getTotalResults();
    	}
    	return (int) queryMgr.search(stringQry, new SearchHandle()).getTotalResults();
    }

    @Override
    public int searchBulk(String q, String collection, Object transaction) {
        StringQueryDefinition stringQry = queryMgr.newStringDefinition();
        if (collection != null) stringQry.setCollections(collection);

        stringQry.setCriteria(q);

        boolean useSearchHandle = Math.random() >= .5; // test using SearchHandle 50% of the time
        SearchHandle searchHandle = useSearchHandle ? new SearchHandle() : null;

        GenericDocumentManager docMgr = collection == null ? genDocMgrNoColl : genDocMgrColl;
        DocumentPage results = docMgr.search(stringQry, 1, searchHandle, (Transaction) transaction);
        results.close();
        return (int) results.getTotalSize();
    }

    @Override
    public long searchPojos(List<Long> ids, String collection, String uriDelta) {
        boolean useSearchHandle = Math.random() >= .5; // test using SearchHandle 50% of the time
        SearchHandle searchHandle = useSearchHandle ? new SearchHandle() : null;

        long countWithOverFiveChildren = 0;
        for ( Long id : ids ) {
            StringQueryDefinition stringQry = queryMgr.newStringDefinition();
            if (collection != null) stringQry.setCollections(collection);
            stringQry.setCriteria(id + uriDelta);
            PojoPage<SamplePojo> results = pojoRepo.search(stringQry, 1, searchHandle);
            if ( results.size() != 1 ) throw new IllegalStateException(
                "Found " + results.size() + " matches for " + id + " but should have found 1");
            SamplePojo result = results.next();
            if ( result.childData != null &&
                 result.childData.childData != null &&
                 result.childData.childData.childData != null &&
                 result.childData.childData.childData.childData != null &&
                 result.childData.childData.childData.childData.childData != null &&
                 result.childData.childData.childData.childData.childData.childData != null &&
                 result.childData.childData.childData.childData.childData.childData.boolData == true )
            {
                countWithOverFiveChildren++;
            }
            results.close();

            PojoQueryDefinition valueQuery = pojoQb.value("id", id);
            if (collection != null) valueQuery.setCollections(collection);
            PojoPage<SamplePojo> results2 = pojoRepo.search(valueQuery, 1, searchHandle);
            if ( results2.size() != 1 ) throw new IllegalStateException(
                "Found " + results2.size() + " matches for id=" + id + " but should have found 1");
            results2.close();
        }

        /*
        PojoQueryDefinition childrenQuery = pojoQb.filteredQuery(
            pojoQb.containerQuery("childData",
                pojoQb.containerQuery("childData",
                    pojoQb.containerQuery("childData",
                        pojoQb.containerQuery("childData",
                            pojoQb.containerQuery("childData",
                                pojoQb.containerQuery("childData",
                                    pojoQb.value("boolData", true))))))));
        if (collection != null) childrenQuery.setCollections(collection);
        PojoPage<SamplePojo> results4 = null;
        long pos = 1;
        do {
            if ( results4 != null ) results4.close();
            System.out.println("DEBUG: [JavaAPISession] pos=[" + pos + "]");
            results4 = pojoRepo.search(childrenQuery, pos, searchHandle);
            // force deserialization of each pojo
            for ( SamplePojo pojo : results4 ) {}
            pos += results4.size();
        } while ( results4.hasNextPage() );
        System.out.println("DEBUG: [JavaAPISession] pos =[" + pos  + "]");
        long countWithOverFiveChildren = pos;
        */
        return countWithOverFiveChildren;
    }

    @Override
    public int kvSearch(String key, String value, String collection, Object transaction) {
        ObjectNode qbe = new ObjectMapper().createObjectNode().put(key, value);
        JacksonHandle qbeHandle = new JacksonHandle(qbe);
        RawQueryByExampleDefinition query = queryMgr.newRawQueryByExampleDefinition(qbeHandle);
        if (collection != null) query.setCollections(collection);
        SearchHandle resultsHandle = queryMgr.search(query, new SearchHandle());

        if (transaction instanceof Transaction) {
            return (int) queryMgr.search(query, new SearchHandle(), (Transaction) transaction).getTotalResults();
        }
        return (int) queryMgr.search(query, new SearchHandle()).getTotalResults();
    }

    @Override
    public int values(String name, String q, String aggregate, Object transaction) {
    	ValuesDefinition valQry = queryMgr.newValuesDefinition(name);

    	StringQueryDefinition stringQry = queryMgr.newStringDefinition();
    	stringQry.setCriteria(q);
    	valQry.setQueryDefinition(stringQry);
    	if (aggregate != null)
    		valQry.setAggregate(aggregate);

    	ValuesHandle valuesHandle =
    		(transaction instanceof Transaction) ?
 			queryMgr.values(valQry, new ValuesHandle(), (Transaction) transaction) :
    		queryMgr.values(valQry, new ValuesHandle());
    	
    	if (aggregate != null) {
    		AggregateResult aggregateResult = valuesHandle.getAggregate(aggregate);
    		if (aggregateResult != null)
    			return aggregateResult.get("xs:int", Integer.class);

    		// IIUC shouldn't be needed

    		AggregateResult[] aggregates = valuesHandle.getAggregates();
            if (aggregates == null || aggregates.length == 0)
            	return 0;

            aggregateResult = aggregates[0];
            return aggregateResult.get("xs:int", Integer.class);
    	}

    	CountedDistinctValue[] values = valuesHandle.getValues();
    	if (values == null || values.length == 0)
    		return 0;

    	CountedDistinctValue value = values[0];
        return (int) value.getCount(); // value.get("xs:int", Integer.class);
    }

    @Override
	public void putRule(String name, String ruledef) {
    	ruleMgr.writeRule(
    			name, new StringHandle(ruledef).withFormat(Format.XML)
    			);
	}

    @Override
    public int match(String content) {
    	return ruleMgr.match(
    			new StringHandle(content), new RuleDefinitionList()
    			).size();
    }

    @Override
    public void delete(String uri, Object transaction) {
    	if (transaction instanceof Transaction) {
        	genDocMgrNoColl.delete(uri, (Transaction) transaction);
    	}
    	genDocMgrNoColl.delete(uri);
    }

    @Override
    public void deleteViaBulk(String[] uris, Object transaction) {
        // not yet implemented
        throw new IllegalStateException("Not yet implemented");
    }

    @Override
    public void deleteViaPojo(Long[] ids, Object transaction) {
        pojoRepo.delete(ids, (Transaction) transaction);
    }

    public class ResourceGetter extends ResourceManager {
    	public ResourceGetter() {
    		super();
    	}
    	public String get(RequestParameters parameters, String contentType) {
    		return getServices().get(
    				parameters,
    				new StringHandle().withMimetype(contentType)
    				).get();
    	}
    }

    @Override
    public void mergeGraph(String graphUri, String contentType, String content, Object transaction) {
        graphManager.merge(graphUri, new StringHandle(content).withMimetype(contentType),
            null, (Transaction) transaction);
    }

    @Override
    public String getMimeType(String uri) {
        if ( uri.endsWith(".ttl") ) {
            return RDFMimeTypes.TURTLE;
        } else if ( uri.endsWith(".nt") ) {
            return RDFMimeTypes.NTRIPLES;
        } else if ( uri.endsWith(".n3") ) {
            return RDFMimeTypes.N3;
        } else if ( uri.endsWith(".rdf") ) {
            return RDFMimeTypes.RDFXML;
        } else if ( uri.endsWith(".json") ) {
            return RDFMimeTypes.RDFJSON;
        } else if ( uri.endsWith(".nq") ) {
            return RDFMimeTypes.NQUADS;
        } else if ( uri.endsWith(".trig") ) {
            return RDFMimeTypes.TRIG;
        } else if ( uri.endsWith(".xml") ) {
            return RDFMimeTypes.TRIPLEXML;
        }
        return null;
    }

    @Override
    public long countGraphsViaRead(Object transaction, String... uris) {
        int count = 0;
        for ( String uri : uris ) {
            String mimetype = getMimeType(uri);
            graphManager.setDefaultMimetype(mimetype);
            String results = graphManager.readAs(uri, String.class);
            if ( results.length() > 0 ) count++;
        }
        return count;
    }

    @Override
    public long countGraphsViaSelect(Object transaction, String... uris) {
        SPARQLQueryManager sparqlManager = client.newSPARQLQueryManager();
        int count = 0;
        int start = 1;
        // we want to stress test receiving some large result sets
        // so pick a number from 1 to 10001 triples
        long pageLength = Math.round(Math.random() * 10000) + 1;
        sparqlManager.setPageLength(pageLength);
        for ( String uri : uris ) {
            String sparql = "SELECT * FROM <" + uri + "> WHERE { ?s ?p ?o }";
            try {
                SPARQLQueryDefinition query = sparqlManager.newQueryDefinition(sparql);
                JsonNode jsonResults = sparqlManager.executeSelect(query, new JacksonHandle(),
                    start, (Transaction) transaction).get();
                int numResults = jsonResults.path("results").path("bindings").size();
                if ( numResults > pageLength ) throw new IllegalStateException(
                    "Retrieved " + numResults + " results but pageLength was " + pageLength);
                // since number of results for this doc might be less than pageLength
                // all we can verify is that we got at least one result
                if ( numResults >= 1 ) count++;
            } catch (Throwable t) {
                message("Failed to retrieve results for SPARQL '" + sparql +
                    "' with pageLength=[" + pageLength + "]: " + t.getMessage());
            }
        }
        return count;
    }

    @Override
    public void deleteGraphs(Object transaction, String... uris) {
        for ( String uri : uris ) {
            graphManager.delete(uri, (Transaction) transaction);
        }
    }
}
