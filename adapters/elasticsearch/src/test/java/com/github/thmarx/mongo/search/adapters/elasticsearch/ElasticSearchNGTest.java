package com.github.thmarx.mongo.search.adapters.elasticsearch;

/*-
 * #%L
 * monog-search-adapters-elasticsearch
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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.github.thmarx.mongo.search.index.MongoSearch;
import com.github.thmarx.mongo.search.index.configuration.FieldConfiguration;
import com.github.thmarx.mongo.search.mapper.FieldMappers;
import com.github.thmarx.mongo.search.mapper.ListFieldMappers;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import com.mongodb.client.result.InsertOneResult;

/**
 *
 * @author t.marx
 */
public class ElasticSearchNGTest extends AbstractContainerTest {

	MongoSearch mongoSearch;

	private MongoDatabase database;

	private ElasticsearchIndexAdapter indexAdapter;

	private final static String COLLECTION_DOKUMENTE = "dokumente";	

	@BeforeMethod
	public void cleanup() throws IOException, InterruptedException {

		database = mongoClient.getDatabase("search");

		if (esClient.indices().exists((fn) -> fn.index(COLLECTION_DOKUMENTE)).value()) {
			esClient.indices().delete((b) -> b.index(COLLECTION_DOKUMENTE));
		}

		esClient.indices().create((b)
				-> b.index(COLLECTION_DOKUMENTE)
						.waitForActiveShards(fn -> fn.count(1))
		);
		
		if (database.getCollection(COLLECTION_DOKUMENTE) != null) {
			database.getCollection(COLLECTION_DOKUMENTE).drop();
		}
		database.createCollection(COLLECTION_DOKUMENTE);
		
		ElasticsearchIndexConfiguration configuration = new ElasticsearchIndexConfiguration();
		configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
				.fieldName("name")
				.indexFieldName("name")
				.mapper(FieldMappers::toString)
				.build()
		);
		configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
				.fieldName("tags")
				.indexFieldName("tags")
				.mapper(ListFieldMappers::toString)
				.build()
		);
		configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, FieldConfiguration.builder()
				.fieldName("cities.name")
				.indexFieldName("cities")
				.mapper(ListFieldMappers::toString)
				.build()
		);

		indexAdapter = new ElasticsearchIndexAdapter(configuration);
		indexAdapter.open(esClient);

		mongoSearch = new MongoSearch();
		mongoSearch.open(indexAdapter, database, List.of(COLLECTION_DOKUMENTE));
		
		Thread.sleep(500);
	}
	
	@AfterMethod
	public void afterMethod() throws Exception {
		mongoSearch.close();
	}

	@Test
	public void test_insert() throws IOException, InterruptedException {
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 1);

		assertCollectionSize(COLLECTION_DOKUMENTE, 1);

		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);

		assertCollectionSize(COLLECTION_DOKUMENTE, 2);
	}
	
	@Test
	public void test_update() throws IOException, InterruptedException {
		
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		
		indexAdapter.commit();
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);
		assertCollectionSize(COLLECTION_DOKUMENTE, 2);

		FindIterable<Document> find = database.getCollection(COLLECTION_DOKUMENTE).find(Filters.empty());

		var doc = find.first();

		var id = doc.getObjectId("_id");
		
		Bson update = Updates.combine(
		Updates.set("tags", List.of("eins", "zwei"))
		);
		
		database.getCollection(COLLECTION_DOKUMENTE).updateOne(Filters.eq("_id", id), update);

		indexAdapter.commit();
		Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> {
			GetResponse<Map> get = esClient.get(fn -> fn.id(id.toString()).index(COLLECTION_DOKUMENTE), Map.class);
			return get.found() && get.source().containsKey("tags");
		});
	}

	@Test
	public void test_delete() throws IOException, InterruptedException {

		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);
		assertCollectionSize(COLLECTION_DOKUMENTE, 2);

		database.getCollection(COLLECTION_DOKUMENTE).deleteMany(Filters.empty());

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 0);
		assertCollectionSize(COLLECTION_DOKUMENTE, 0);
	}
	
	@Test
	public void test_dropCollection() throws IOException, InterruptedException {

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
		InsertOneResult insertOne = collection.insertOne(document);
	}

	private void deleteDocument(final String collectionName, final String uid) {
		MongoCollection<Document> collection = database.getCollection(collectionName);
		collection.deleteOne(Filters.eq("_id", new ObjectId(uid)));
	}

	private long getSize(final String collection) throws IOException {
		if (!esClient.indices().exists((fn) -> fn.index(collection)).value()) {
			return 0;
		}

		CountResponse count = esClient.count((i) -> i.index(collection));
		return count.count();
	}

	private void assertCollectionSize(final String collection, int size) throws IOException {
		long count = getSize(collection);
		Assertions.assertThat(count).isEqualTo(size);
	}

}
