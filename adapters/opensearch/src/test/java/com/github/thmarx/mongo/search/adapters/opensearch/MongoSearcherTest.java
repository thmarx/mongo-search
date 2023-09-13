package com.github.thmarx.mongo.search.adapters.opensearch;

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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.github.thmarx.mongo.search.index.MongoSearch;
import com.github.thmarx.mongo.search.index.commands.InitializeCommand;
import com.github.thmarx.mongo.search.index.configuration.FieldConfiguration;
import com.github.thmarx.mongo.search.mapper.FieldMappers;
import com.github.thmarx.mongo.search.mapper.ListFieldMappers;
import com.mongodb.client.FindIterable;
import com.mongodb.client.model.Updates;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.CountResponse;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author t.marx
 */
public class MongoSearcherTest extends AbstractContainerTest {

	static String PASSWORD = "AgY7KUJwb5M*e36Y3=gb";

	MongoClient client;

	MongoSearch mongoSearch;

	private MongoDatabase database;

	private OpensearchIndexAdapter indexAdapter;

	OpenSearchClient osClient;

	private final static String COLLECTION_DOKUMENTE = "dokumente";

	@BeforeClass
	public void setup() throws IOException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(opensearchContainer.getUsername(), opensearchContainer.getPassword());
		credentialsProvider.setCredentials(AuthScope.ANY, credentials);

		final HttpHost host = HttpHost.create(opensearchContainer.getHttpHostAddress());

		final SSLContext sslcontext = SSLContextBuilder.create()
            .loadTrustMaterial(null, new TrustAllStrategy())
            .build();
		//Initialize the client with SSL and TLS enabled
		final RestClient restClient = RestClient.builder(host).
				setHttpClientConfigCallback((HttpAsyncClientBuilder httpClientBuilder) -> 
					httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setSSLContext(sslcontext)
		).build();

		final OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
		this.osClient = new OpenSearchClient(transport);

		//client = MongoClients.create(connectionString);
		client = MongoClients.create(mongdbContainer.getConnectionString());

		database = client.getDatabase("search");
	}

	@AfterClass
	public void shutdown() throws Exception {
		client.close();
		osClient.shutdown();
	}

	@BeforeMethod
	public void cleanup() throws IOException {
		if (osClient.indices().exists((fn) -> fn.index(COLLECTION_DOKUMENTE)).value()) {
			osClient.indices().delete((b) -> b.index(COLLECTION_DOKUMENTE));
		}

		osClient.indices().create((b)
				-> b.index(COLLECTION_DOKUMENTE)
						.waitForActiveShards(fn -> fn.count(1))
		);

		if (database.getCollection(COLLECTION_DOKUMENTE) != null) {
			database.getCollection(COLLECTION_DOKUMENTE).drop();
		}
		database.createCollection(COLLECTION_DOKUMENTE);

		OpensearchIndexConfiguration configuration = new OpensearchIndexConfiguration();
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

		indexAdapter = new OpensearchIndexAdapter(configuration);
		indexAdapter.open(osClient);

		mongoSearch = new MongoSearch();
		mongoSearch.open(indexAdapter, database, List.of(COLLECTION_DOKUMENTE));
		mongoSearch.execute(new InitializeCommand(List.of(COLLECTION_DOKUMENTE)));
	}

	@AfterMethod
	public void afterMethod() throws Exception {
		mongoSearch.close();
	}

	@Test
	public void test_insert() throws IOException, InterruptedException {

		Thread.sleep(2000);

		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 1);

		assertCollectionSize(COLLECTION_DOKUMENTE, 1);

		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);

		assertCollectionSize(COLLECTION_DOKUMENTE, 2);
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
	public void test_update() throws IOException, InterruptedException {

		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);
		assertCollectionSize(COLLECTION_DOKUMENTE, 2);

		FindIterable<Document> find = database.getCollection(COLLECTION_DOKUMENTE).find(Filters.empty());

		var doc = find.first();

		var id = doc.getObjectId("_id");

		Bson update = Updates.combine(
				Updates.set("tags", List.of("eins", "zwei"))
		);

		database.getCollection(COLLECTION_DOKUMENTE).updateOne(Filters.eq("_id", id), update);

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
			GetResponse<Map> get = osClient.get(fn -> fn.id(id.toString()).index(COLLECTION_DOKUMENTE), Map.class);
			return get.found() && get.source().containsKey("tags");
		});
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
		collection.insertOne(document);
	}

	private void deleteDocument(final String collectionName, final String uid) {
		MongoCollection<Document> collection = database.getCollection(collectionName);
		collection.deleteOne(Filters.eq("_id", new ObjectId(uid)));
	}

	private long getSize(final String collection) throws IOException {
		if (!osClient.indices().exists((fn) -> fn.index(collection)).value()) {
			return 0;
		}

		CountResponse count = osClient.count((i) -> i.index(collection));
		return count.count();
	}

	private void assertCollectionSize(final String collection, int size) throws IOException {
		long count = getSize(collection);
		Assertions.assertThat(count).isEqualTo(size);
	}

}
