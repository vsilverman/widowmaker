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

import java.util.List;
import java.util.Map;

public interface RestSession {
	public Object beginTransaction();
	public void commitTransaction(Object transaction);
	public void rollbackTransaction(Object transaction);

	public Map<String, List<String>> makeTransform(
			String transformName, Map<String, List<String>> transformParams
			);

	public void putDocument(String uri, byte[] content, Object transaction);
    public void putDocument(String uri, String collection, byte[] content, Object transaction);
    public void putDocument(String uri, String contentType, String collection, byte[] content, Object transaction);
	public void putDocument(String uri, byte[] content, Map<String, List<String>> transform, Object transaction);
    public void putPojo(SamplePojo content, Object transaction, String... collections);
    public void putDocuments(List<String> uris, List<String> contentType, List<byte[]> contents,
        List<String> collections, Map<String, List<String>> transform, Object transaction);
    public void putDocumentsViaEval(List<String> uris, List<String> contentTypes, List<byte[]> contents,
        List<String> collections, Object transaction);
    public void putDocumentViaExtenstion(String uri, String contentType, byte[] content,
        String collection, Object transaction);

	public void patch(String uri, byte[] change, Object transaction);

	public boolean docExists(String uri, Object transaction);
    public long countDocumentsViaBulk(Object transaction, String... uris);
    public long countDocumentsViaPojo(Object transaction, Long... ids);
    public long countDocumentsViaEval(Object transaction, String... uris);
    public long countDocumentsViaExtension(Object transaction, String... uris);

    public String getDocument(String uri, Object transaction);
    public String getDocument(String uri, Map<String, List<String>> transform, Object transaction);

    public String getResource(String name, String contentType, String... params);

    public int search(String q, String collection, Object transaction);
    public int searchBulk(String q, String collection, Object transaction);
    public long searchPojos(List<Long> ids, String collection, String uriDelta);

    public int kvSearch(String key, String value, String collection, Object transaction);

    public int values(String name, String q, String aggregate, Object transaction);

	public void putRule(String uri, String ruledef);
    public int match(String content);

    public void delete(String uri, Object transaction);
    public void deleteViaBulk(String[] uri, Object transaction);
    public void deleteViaPojo(Long[] uri, Object transaction);

    public void mergeGraph(String graphUri, String contentType, String content, Object transaction);
    public String getMimeType(String uri);
    public long countGraphsViaRead(Object transaction, String... uris);
    public long countGraphsViaSelect(Object transaction, String... uris);
    public void deleteGraphs(Object transaction, String... uris);
}
