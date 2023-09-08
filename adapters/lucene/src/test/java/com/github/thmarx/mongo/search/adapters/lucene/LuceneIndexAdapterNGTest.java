package com.github.thmarx.mongo.search.adapters.lucene;

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
import com.github.thmarx.mongo.search.adapters.lucene.index.LuceneIndexConfiguration;
import com.github.thmarx.mongo.search.adapters.lucene.index.storage.FileSystemStorage;
import com.github.thmarx.mongo.search.index.MongoSearch;
import com.github.thmarx.mongo.search.index.configuration.FieldConfiguration;
import com.github.thmarx.mongo.search.mapper.FieldMappers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author t.marx
 */
public class LuceneIndexAdapterNGTest extends AbstractContainerTest {

	MongoClient client;

	MongoSearch mongoSearch;

	private MongoDatabase database;

	private LuceneIndexAdapter luceneIndexAdapter;
	
	private FacetsConfig facetConfig = new FacetsConfig();

	@BeforeMethod
	public void setup() throws IOException {

		FileUtil.delete(Path.of("target/index"));
		Files.createDirectories(Path.of("target/index"));

		client = MongoClients.create(mongdbContainer.getConnectionString());

		database = client.getDatabase("search");

		if (database.getCollection("dokumente") != null) {
			database.getCollection("dokumente").drop();
		}
		database.createCollection("dokumente");

		facetConfig.setMultiValued("tags", true);
		LuceneIndexConfiguration configuration = new LuceneIndexConfiguration();
		configuration.setDefaultAnalyzer(new StandardAnalyzer());
		configuration.setStorage(new FileSystemStorage(Path.of("target/index")));
		configuration.setFacetsConfig(facetConfig);
		
		configuration.addFieldConfiguration("dokumente", FieldConfiguration.<Document, org.apache.lucene.document.Document>builder()
				.fieldName("name")
				.indexFieldName("name")
				.mapper(FieldMappers::getStringFieldValue)
				.build()
		);
		
		
		facetConfig.setMultiValued("tags", true);
		configuration.addFieldConfiguration("dokumente", FieldConfiguration.<Document, org.apache.lucene.document.Document>builder()
				.fieldName("tags")
				.indexFieldName("tags")
				.mapper(FieldMappers::getStringArrayFieldValue)
				.extender((source, target) -> {
					var values = FieldMappers.getStringArrayFieldValue("tags", source);
					if (values != null && !values.isEmpty()) {
						values.forEach(value -> {
							target.add(new SortedSetDocValuesFacetField("tags", value));
						});
					}
				})
				.build()
		);
		configuration.addFieldConfiguration("dokumente", FieldConfiguration.<Document, org.apache.lucene.document.Document>builder()
				.fieldName("cities.name")
				.indexFieldName("cities")
				.mapper(FieldMappers::getStringArrayFieldValue)
				.build()
		);

		luceneIndexAdapter = new LuceneIndexAdapter(configuration);
		luceneIndexAdapter.open();

		mongoSearch = new MongoSearch();
		mongoSearch.open(luceneIndexAdapter, database, List.of("dokumente", "bilder"));
	}

	@AfterMethod
	public void shutdown() throws Exception {
		client.close();
		mongoSearch.close();
	}

	@Test
	public void test_insert() throws IOException, InterruptedException {

		Thread.sleep(2000);

		luceneIndexAdapter.commit();

		assertCollectionSize("dokumente", 0);

		insertDocument("dokumente", Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize("dokumente") > 0);

		assertCollectionSize("dokumente", 1);

		insertDocument("dokumente", Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize("dokumente") > 1);

		assertCollectionSize("dokumente", 2);
	}

	@Test
	public void test_delete() throws IOException, InterruptedException {

		Thread.sleep(2000);

		luceneIndexAdapter.commit();

		assertCollectionSize("dokumente", 0);

		insertDocument("dokumente", Map.of("name", "thorsten"));
		insertDocument("dokumente", Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize("dokumente") == 2);
		
		IndexSearcher searcher = luceneIndexAdapter.getIndex().getSearcherManager().acquire();
		try {
			var uid = searcher.getIndexReader().storedFields().document(0).get("_id");
			deleteDocument("dokumente", uid);
		} finally {
			luceneIndexAdapter.getIndex().getSearcherManager().release(searcher);
		}
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize("dokumente") == 1);
		assertCollectionSize("dokumente", 1);
	}
	
	@Test
	public void test_mapper() throws IOException, InterruptedException {

		Thread.sleep(2000);

		luceneIndexAdapter.commit();

		assertCollectionSize("dokumente", 0);

		insertDocument("dokumente", Map.of(
				"name", "thorsten",
				"tags", List.of("eins", "zwei"),
				"cities", List.of(
						Map.of("name", "Bochum"),
						Map.of("name", "Dortmund"),
						Map.of("name", "Essen")
				)
		));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize("dokumente") > 0);

		IndexReader reader = DirectoryReader.open(FSDirectory.open(Path.of("target/index")));
		try {
			
			org.apache.lucene.document.Document doc = reader.storedFields().document(0);
			
			Assertions.assertThat(doc.get("_collection")).isNotNull();
			Assertions.assertThat(doc.get("_id")).isNotNull();
			
			Assertions.assertThat(doc.get("name")).isNotNull().isEqualTo("thorsten");
			
			Assertions.assertThat(doc.get("tags")).isNotNull();
			Assertions.assertThat(doc.getValues("tags"))
					.isNotNull()
					.hasSize(2)
					.containsExactlyInAnyOrder("eins", "zwei");
			
			Assertions.assertThat(doc.get("cities")).isNotNull();
			Assertions.assertThat(doc.getValues("cities"))
					.isNotNull()
					.hasSize(3)
					.containsExactlyInAnyOrder("Bochum", "Essen", "Dortmund");
		} finally {
			reader.close();
		}
	}
	
	@Test
	public void test_extender() throws IOException, InterruptedException {

		Thread.sleep(2000);

		luceneIndexAdapter.commit();

		assertCollectionSize("dokumente", 0);

		insertDocument("dokumente", Map.of(
				"name", "thorsten",
				"tags", List.of("eins", "zwei")
		));
		insertDocument("dokumente", Map.of(
				"name", "lara",
				"tags", List.of("drei", "zwei")
		));

		Awaitility.await().atMost(10, TimeUnit.MINUTES).until(() -> getSize("dokumente") == 2);

		luceneIndexAdapter.commit();
		
		Set<String> tags = getSortedSetFacet("dokumente", "tags");
		Assertions.assertThat(tags)
					.isNotNull()
					.hasSize(3)
					.containsExactlyInAnyOrder("eins", "zwei", "drei");
	}
	
	private Set<String> getSortedSetFacet(final String collectionName, final String fieldName) throws IOException {
		IndexSearcher searcher = luceneIndexAdapter.getIndex().getSearcherManager().acquire();
		try {
			/*
			SortedSetDocValues sortedSetValues = MultiDocValues.getSortedSetValues(searcher.getIndexReader(), fieldName);
			if (sortedSetValues == null || sortedSetValues.nextDoc() == SortedSetDocValues.NO_MORE_DOCS) {
				return Collections.emptySet();
			}
			 */
			
			Set<String> fieldValues = new HashSet<>();
					
			SortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(searcher.getIndexReader(), facetConfig);
			
			FacetsCollector collector = new FacetsCollector();
			
			TermQuery collectionQuery = new TermQuery(new Term("_collection", collectionName));
			
			FacetsCollector.search(searcher, collectionQuery, 10, collector);
			Facets facets = new SortedSetDocValuesFacetCounts(state, collector);
			FacetResult allChildren = facets.getAllChildren(fieldName);

			Stream.of(allChildren.labelValues).forEach(lv -> fieldValues.add(lv.label));
			
			return fieldValues;
		} finally {
			luceneIndexAdapter.getIndex().getSearcherManager().release(searcher);
		}
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

	private int getSize(final String collection) throws IOException {

		return luceneIndexAdapter.size("search", collection);
	}

	private void assertCollectionSize(final String collection, int size) throws IOException {
		int count = getSize(collection);
		Assertions.assertThat(count).isEqualTo(size);
	}

}
