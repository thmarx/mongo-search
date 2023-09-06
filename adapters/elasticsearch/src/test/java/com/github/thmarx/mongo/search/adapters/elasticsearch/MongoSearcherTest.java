package com.github.thmarx.mongo.search.adapters.elasticsearch;

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
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.github.thmarx.mongo.search.index.MongoSearch;
import com.github.thmarx.mongo.search.index.configuration.FieldConfiguration;
import com.github.thmarx.mongo.search.retriever.FieldValueRetrievers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.elasticsearch.client.RestClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author t.marx
 */
public class MongoSearcherTest {

	static String PASSWORD = "AgY7KUJwb5M*e36Y3=gb";

	MongoClient client;

	MongoSearch mongoSearch;

	private MongoDatabase database;

	private ElasticsearchIndexAdapter indexAdapter;

	RestClient restClient;
	ElasticsearchTransport transport;
	ElasticsearchClient esClient;

	ElasticsearchContainer container;

//	@BeforeClass
	public void up() {
		container = new ElasticsearchContainer(DockerImageName.parse(
				"docker.elastic.co/elasticsearch/elasticsearch:8.9.1-amd64"
		));
		container.start();
	}
//	@AfterClass
	public void down () {
		container.stop();
	}

	@BeforeMethod
	public void setup() throws IOException {

		restClient = RestClient
				.builder(HttpHost.create(/*container.getHttpHostAddress()*/ "https://testcluster-6975475491.eu-central-1.bonsaisearch.net:443"))
				.setHttpClientConfigCallback((clientBuilder) -> {

					final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
					//UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("elastic", ElasticsearchContainer.ELASTICSEARCH_DEFAULT_PASSWORD);
					UsernamePasswordCredentials credentials = new UsernamePasswordCredentials("b2wvxs2gpd", "qm8ytnecz1");
					credentialsProvider.setCredentials(AuthScope.ANY, credentials);

					return clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
				})
				.build();

// Create the transport with a Jackson mapper
		transport = new RestClientTransport(
				restClient, new JacksonJsonpMapper());

// And create the API client
		esClient = new ElasticsearchClient(transport);

		String connectionString = System.getenv("MONGO_SEARCH_CONNECTIONSTRING");

		FileUtil.delete(Path.of("target/index"));
		Files.createDirectories(Path.of("target/index"));

		client = MongoClients.create(connectionString);

		database = client.getDatabase("search");

		if (database.getCollection("dokumente") != null) {
			database.getCollection("dokumente").drop();
		}
		database.createCollection("dokumente");

		ElasticsearchIndexConfiguration configuration = new ElasticsearchIndexConfiguration();
		configuration.addFieldConfiguration("dokumente", FieldConfiguration.builder()
				.fieldName("name")
				.indexFieldName("name")
				.retriever(FieldValueRetrievers::getStringFieldValue)
				.build()
		);
		configuration.addFieldConfiguration("dokumente", FieldConfiguration.builder()
				.fieldName("tags")
				.indexFieldName("tags")
				.retriever(FieldValueRetrievers::getStringArrayFieldValue)
				.build()
		);
		configuration.addFieldConfiguration("dokumente", FieldConfiguration.builder()
				.fieldName("cities.name")
				.indexFieldName("cities")
				.retriever(FieldValueRetrievers::getStringArrayFieldValue)
				.build()
		);

		indexAdapter = new ElasticsearchIndexAdapter(configuration);
		indexAdapter.open(esClient);

		mongoSearch = new MongoSearch();
		mongoSearch.open(indexAdapter, database, List.of("dokumente", "bilder"));
	}

	@AfterMethod
	public void shutdown() throws Exception {
		client.close();
		mongoSearch.close();

		esClient.shutdown();
	}

	@Test
	public void test_insert() throws IOException, InterruptedException {

		Thread.sleep(2000);

		indexAdapter.commit();

		insertDocument("dokumente", Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize("dokumente") > 0);

		assertCollectionSize("dokumente", 1);

		insertDocument("dokumente", Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize("dokumente") > 1);

		assertCollectionSize("dokumente", 2);
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
		CountResponse count = esClient.count((i) -> i.index("dokumente"));
		return count.count();
	}

	private void assertCollectionSize(final String collection, int size) throws IOException {
		long count = getSize(collection);
		Assertions.assertThat(count).isEqualTo(size);
	}

}
