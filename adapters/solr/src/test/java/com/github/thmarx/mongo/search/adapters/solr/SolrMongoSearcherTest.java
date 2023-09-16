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
import com.github.thmarx.mongo.search.index.MongoSearch;
import com.github.thmarx.mongo.search.index.configuration.FieldConfiguration;
import com.github.thmarx.mongo.search.mapper.FieldMappers;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author t.marx
 */
public class SolrMongoSearcherTest extends AbstractContainerTest {

	MongoSearch mongoSearch;

	SolrIndexAdapter adapter;

	private final static String COLLECTION_DOKUMENTE = "dokumente";

	@BeforeMethod
	public void beforeMethod() throws IOException, SolrServerException {
		if (database.getCollection(COLLECTION_DOKUMENTE) != null) {
			database.getCollection(COLLECTION_DOKUMENTE).drop();
		}
		database.createCollection(COLLECTION_DOKUMENTE);

		SolrIndexConfiguration indexConfiguration = new SolrIndexConfiguration();
		indexConfiguration.setCommitWithin(1000);
		indexConfiguration.addFieldConfiguration(COLLECTION_DOKUMENTE,
				FieldConfiguration.builder()
						.fieldName("name")
						.indexFieldName("name")
						.mapper(FieldMappers::toString)
						.build());

		adapter = new SolrIndexAdapter(indexConfiguration);
		adapter.open(solrClient);

		mongoSearch = new MongoSearch();
		mongoSearch.open(adapter, database, List.of(COLLECTION_DOKUMENTE));

		CollectionAdminRequest col = CollectionAdminRequest.createCollection("dokumente", 1, 1);

		NamedList<Object> request = solrClient.request(col);
	}

	@AfterMethod
	public void afterMethod() throws Exception, SolrServerException {
		adapter.close();
		mongoSearch.close();

		CollectionAdminRequest col = CollectionAdminRequest.deleteCollection(COLLECTION_DOKUMENTE);
		NamedList<Object> request = solrClient.request(col);
	}

	@Test
	public void test_insert() throws IOException, InterruptedException, SolrServerException {

		Thread.sleep(2000);

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);

		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) > 0);

		assertCollectionSize(COLLECTION_DOKUMENTE, 1);

		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) > 1);

		assertCollectionSize(COLLECTION_DOKUMENTE, 2);
	}
	
	@Test
	public void test_clear() throws IOException, InterruptedException, SolrServerException {

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);
		assertCollectionSize(COLLECTION_DOKUMENTE, 2);

		database.getCollection(COLLECTION_DOKUMENTE).deleteMany(Filters.empty());
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 0);
		assertCollectionSize(COLLECTION_DOKUMENTE, 0);
	}
	
	@Test
	public void test_delete_single() throws IOException, InterruptedException, SolrServerException {

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);
		assertCollectionSize(COLLECTION_DOKUMENTE, 2);

		Document toDelete = database.getCollection(COLLECTION_DOKUMENTE).find(Filters.empty()).first();
		deleteDocument(COLLECTION_DOKUMENTE, toDelete.getObjectId("_id").toString());
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 1);
		assertCollectionSize(COLLECTION_DOKUMENTE, 1);
	}
	
	@Test
	public void test_update() throws IOException, InterruptedException, SolrServerException {

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 1);
		assertCollectionSize(COLLECTION_DOKUMENTE, 1);

		Document toDelete = database.getCollection(COLLECTION_DOKUMENTE).find(Filters.empty()).first();
		var uid = toDelete.getObjectId("_id").toString();
		Bson update = Updates.combine(
		Updates.set("name", "lara")
		);
		database.getCollection(COLLECTION_DOKUMENTE)
				.updateOne(Filters.eq("_id", toDelete.getObjectId("_id")), update);
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
				var doc = getFromIndex(COLLECTION_DOKUMENTE, uid); 
				return doc.getFirstValue("name").equals("lara");
		});
		assertCollectionSize(COLLECTION_DOKUMENTE, 1);
	}
	
	@Test
	public void test_drop_collection() throws IOException, InterruptedException, SolrServerException {

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);
		assertCollectionSize(COLLECTION_DOKUMENTE, 2);

		database.getCollection(COLLECTION_DOKUMENTE).drop();
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 0);
		assertCollectionSize(COLLECTION_DOKUMENTE, 0);
	}

	private void insertDocument(final String collectionName, final Map attributes) {
		MongoCollection<Document> collection = database.getCollection(collectionName);
		Document document = new Document(attributes);
		collection.insertOne(document);
	}

	private void deleteDocument(final String collectionName, final String uid) {
		MongoCollection<Document> collection = database.getCollection(collectionName);
		collection.deleteOne(Filters.eq("_id", new ObjectId(uid)));
	}

	private SolrDocument getFromIndex(final String collection, final String uid) throws IOException, SolrServerException {
		return solrClient.getById(collection, uid);
	}
	
	private long getSize(final String collection) throws IOException, SolrServerException {
		SolrQuery query = new SolrQuery("*:*");
		final QueryResponse response = solrClient.query(collection, query);
		
		return response.getResults().getNumFound();
	}

	private void assertCollectionSize(final String collection, int size) throws IOException, SolrServerException {
		long count = getSize(collection);
		Assertions.assertThat(count).isEqualTo(size);
	}
}
