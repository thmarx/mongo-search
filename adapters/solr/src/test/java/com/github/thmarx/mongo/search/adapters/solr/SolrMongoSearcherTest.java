package com.github.thmarx.mongo.search.adapters.solr;

/*-
 * #%L
 * mongo-search-index
 * %%
 * Copyright (C) 2023 Marx-Software
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.github.thmarx.mongo.search.index.MongoSearch;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author t.marx
 */
public class SolrMongoSearcherTest extends AbstractContainerTest {

	MongoClient client;

	MongoSearch mongoSearch;

	private MongoDatabase database;

	private final static String COLLECTION_DOKUMENTE = "dokumente";

	SolrClient solrClient;

	@BeforeClass
	public void setup() throws IOException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

		client = MongoClients.create(mongdbContainer.getConnectionString());
		database = client.getDatabase("search");

		// Do whatever you want with the client ...
		solrClient = new Http2SolrClient.Builder(
				"http://" + solrContainer.getHost() + ":" + solrContainer.getSolrPort() + "/solr"
		).build();
	}

	@AfterClass
	public void shutdown() throws Exception {
		client.close();
	}

	@BeforeMethod
	public void cleanup() throws IOException {

	}

	@AfterMethod
	public void afterMethod() throws Exception {
	}

	@Test
	public void test_insert() throws IOException, InterruptedException, SolrServerException {

		Thread.sleep(2000);

		System.out.println("TEST");
		
		SolrPingResponse response = solrClient.ping("dummy");
		System.out.println("response: " + response.jsonStr());
		
		CollectionAdminRequest col = CollectionAdminRequest.createCollection("dokumente", 1, 1);
		
		NamedList<Object> request = solrClient.request(col);
		System.out.println(request);
		
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("id", UUID.randomUUID().toString());
		doc.addField("name", "thorsten");
		solrClient.add("dokumente", doc);
	}
}
