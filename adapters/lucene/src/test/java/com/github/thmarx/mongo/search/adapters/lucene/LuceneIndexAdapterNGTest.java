package com.github.thmarx.mongo.search.adapters.lucene;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
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

import com.github.thmarx.mongo.search.adapters.lucene.index.LuceneFieldConfiguration;
import com.github.thmarx.mongo.search.adapters.lucene.index.LuceneIndexConfiguration;
import com.github.thmarx.mongo.search.adapters.lucene.index.storage.FileSystemStorage;
import com.github.thmarx.mongo.search.index.MongoSearch;
import com.github.thmarx.mongo.search.index.commands.InitializeCommand;
import com.github.thmarx.mongo.search.mapper.FieldMappers;
import com.github.thmarx.mongo.search.mapper.ListFieldMappers;
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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 *
 * @author t.marx
 */
public class LuceneIndexAdapterNGTest extends AbstractContainerTest {

	MongoSearch mongoSearch;

	private MongoDatabase database;

	private LuceneIndexAdapter luceneIndexAdapter;

	private FacetsConfig facetConfig = new FacetsConfig();
	
	private static final String COLLECTION_DOKUMENTE = "dokumente";
	private static final String DB_SEARCH = "search";

	@BeforeMethod
	public void setup() throws IOException, InterruptedException {

		database = mongoClient.getDatabase(DB_SEARCH);

		FileUtil.delete(Path.of("target/index"));
		Files.createDirectories(Path.of("target/index"));

		if (database.getCollection(COLLECTION_DOKUMENTE) != null) {
			database.getCollection(COLLECTION_DOKUMENTE).drop();
		}
		database.createCollection(COLLECTION_DOKUMENTE);

		facetConfig.setMultiValued("tags", true);
		LuceneIndexConfiguration configuration = new LuceneIndexConfiguration();

		PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(new StandardAnalyzer(),
				Map.of("name", new GermanAnalyzer()));
		configuration.setDefaultAnalyzer(perFieldAnalyzerWrapper);
		configuration.setCommitDelaySeconds(1);
		configuration.setStorage(new FileSystemStorage(Path.of("target/index")));
		configuration.setDefaultFacetsConfig(facetConfig);
		configuration.setDocumentExtender((source, target) -> {
			var values = ListFieldMappers.toString("tags", source);
			if (values != null && !values.isEmpty()) {
				values.forEach(value -> {
					target.add(new SortedSetDocValuesFacetField("tags", value));
				});
			}
		});

		configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, LuceneFieldConfiguration.builder()
				.fieldName("name")
				.indexFieldName("name")
				.stored(true)
				.mapper(FieldMappers::toString)
				.build());

		facetConfig.setMultiValued("tags", true);
		configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, LuceneFieldConfiguration.builder()
				.fieldName("tags") 
				.indexFieldName("tags")
				.stored(true)
				.keyword(true)
				.mapper(ListFieldMappers::toString)
				.build());
		configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, LuceneFieldConfiguration.builder()
				.fieldName("cities.name")
				.indexFieldName("cities")
				.defaultValue(() -> "K-Town")
				.stored(true)
				.mapper(ListFieldMappers::toString)
				.build());

		configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, LuceneFieldConfiguration.builder()
				.fieldName("ort.name")
				.indexFieldName("cities")
				.stored(true)
				.mapper(FieldMappers::toString)
				.build());

		configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, LuceneFieldConfiguration.builder()
				.fieldName("created")
				.indexFieldName("created")
				.stored(true)
				.dateFormatter(DateTimeFormatter.ISO_LOCAL_DATE)
				.mapper(FieldMappers::toDate)
				.build());
		configuration.addFieldConfiguration(COLLECTION_DOKUMENTE, LuceneFieldConfiguration.builder()
				.fieldName("draft")
				.indexFieldName("draft")
				.stored(true)
				.mapper(FieldMappers::toBoolean)
				.build());

		luceneIndexAdapter = new LuceneIndexAdapter(configuration);
		luceneIndexAdapter.open();

		mongoSearch = new MongoSearch();
		mongoSearch.open(luceneIndexAdapter, database, List.of(COLLECTION_DOKUMENTE, "bilder"));
		mongoSearch.execute(new InitializeCommand(List.of(COLLECTION_DOKUMENTE, "bilder")));
		
		Thread.sleep(500);
	}

	@AfterMethod
	public void shutdown() throws Exception {
		mongoSearch.close();
	}

	@Test
	public void test_insert() throws IOException, InterruptedException {

		luceneIndexAdapter.commit();

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);

		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 1);

		assertCollectionSize(COLLECTION_DOKUMENTE, 1);

		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);

		assertCollectionSize(COLLECTION_DOKUMENTE, 2);
	}

	@Test
	public void test_delete() throws IOException, InterruptedException {

		luceneIndexAdapter.commit();

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);

		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));
		insertDocument(COLLECTION_DOKUMENTE, Map.of("name", "thorsten"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);

		IndexSearcher searcher = luceneIndexAdapter.getIndex(DB_SEARCH, COLLECTION_DOKUMENTE).getSearcherManager().acquire();
		try {
			var uid = searcher.getIndexReader().storedFields().document(0).get("_id");
			deleteDocument(COLLECTION_DOKUMENTE, uid);
		} finally {
			luceneIndexAdapter.getIndex(DB_SEARCH, COLLECTION_DOKUMENTE).getSearcherManager().release(searcher);
		}

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 1);
		assertCollectionSize(COLLECTION_DOKUMENTE, 1);
	}

	@Test
	public void test_mapper() throws IOException, InterruptedException {

		luceneIndexAdapter.commit();

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);

		insertDocument(COLLECTION_DOKUMENTE, Map.of(
				"name", "thorsten",
				"tags", List.of("eins", "zwei"),
				"cities", List.of(
						Map.of("name", "Bochum"),
						Map.of("name", "Dortmund"),
						Map.of("name", "Essen"))));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) > 0);

		IndexReader reader = DirectoryReader.open(FSDirectory.open(Path.of("target/index/dokumente")));
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
	public void test_date() throws IOException, InterruptedException {

		luceneIndexAdapter.commit();

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);

		insertDocument(COLLECTION_DOKUMENTE, Map.of(
				"name", "thorsten",
				"created", LocalDateTime.now()));

		var indexedDateValue = DateTimeFormatter.ISO_LOCAL_DATE.format(LocalDateTime.now());

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 1);

		IndexReader reader = DirectoryReader.open(FSDirectory.open(Path.of("target/index/dokumente")));
		try {

			org.apache.lucene.document.Document doc = reader.storedFields().document(0);

			Assertions.assertThat(doc.get("_collection")).isNotNull();
			Assertions.assertThat(doc.get("_id")).isNotNull();

			Assertions.assertThat(doc.get("created")).isNotNull().isEqualTo(indexedDateValue);
		} finally {
			reader.close();
		}
	}

	@Test
	public void test_boolean() throws IOException, InterruptedException {

		luceneIndexAdapter.commit();

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);

		insertDocument(COLLECTION_DOKUMENTE, Map.of(
				"name", "thorsten",
				"draft", false));

		insertDocument(COLLECTION_DOKUMENTE, Map.of(
				"name", "thorsten",
				"draft", true));
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);

		IndexReader reader = DirectoryReader.open(FSDirectory.open(Path.of("target/index/dokumente")));
		try {

			org.apache.lucene.document.Document doc = reader.storedFields().document(0);
			Assertions.assertThat(doc.get("draft")).isNotNull().isEqualTo("false");

			doc = reader.storedFields().document(1);
			Assertions.assertThat(doc.get("draft")).isNotNull().isEqualTo("true");
		} finally {
			reader.close();
		}
	}

	@Test
	public void test_combine_fields() throws IOException, InterruptedException {

		luceneIndexAdapter.commit();

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);

		insertDocument(COLLECTION_DOKUMENTE, Map.of(
				"name", "thorsten",
				"tags", List.of("eins", "zwei"),
				"cities", List.of(
						Map.of("name", "Bochum"),
						Map.of("name", "Dortmund"),
						Map.of("name", "Essen")),
				"ort", Map.of("name", "Kaiserslautern")));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) > 0);

		IndexReader reader = DirectoryReader.open(FSDirectory.open(Path.of("target/index/dokumente")));
		try {

			org.apache.lucene.document.Document doc = reader.storedFields().document(0);

			Assertions.assertThat(doc.get("cities")).isNotNull();
			Assertions.assertThat(doc.getValues("cities"))
					.isNotNull()
					.hasSize(4)
					.containsExactlyInAnyOrder("Bochum", "Essen", "Dortmund", "Kaiserslautern");
		} finally {
			reader.close();
		}
	}

	@Test
	public void test_default_value() throws IOException, InterruptedException {

		Thread.sleep(2000);

		luceneIndexAdapter.commit();

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);

		insertDocument(COLLECTION_DOKUMENTE, Map.of(
				"name", "thorsten",
				"tags", List.of("eins", "zwei")));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) > 0);

		IndexReader reader = DirectoryReader.open(FSDirectory.open(Path.of("target/index/dokumente")));
		try {

			org.apache.lucene.document.Document doc = reader.storedFields().document(0);

			Assertions.assertThat(doc.get("cities")).isNotNull();
			Assertions.assertThat(doc.getValues("cities"))
					.isNotNull()
					.hasSize(1)
					.containsExactlyInAnyOrder("K-Town");
		} finally {
			reader.close();
		}
	}

	@Test
	public void test_extender() throws IOException, InterruptedException {

		Thread.sleep(2000);

		luceneIndexAdapter.commit();

		assertCollectionSize(COLLECTION_DOKUMENTE, 0);

		insertDocument(COLLECTION_DOKUMENTE, Map.of(
				"name", "thorsten",
				"tags", List.of("eins", "zwei")));
		insertDocument(COLLECTION_DOKUMENTE, Map.of(
				"name", "lara",
				"tags", List.of("drei", "zwei")));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOKUMENTE) == 2);

		luceneIndexAdapter.commit();

		Set<String> tags = getSortedSetFacet("tags");
		Assertions.assertThat(tags)
				.isNotNull()
				.hasSize(3)
				.containsExactlyInAnyOrder("eins", "zwei", "drei");
	}

	private Set<String> getSortedSetFacet(final String fieldName) throws IOException {
		IndexSearcher searcher = luceneIndexAdapter.getIndex(DB_SEARCH, COLLECTION_DOKUMENTE).getSearcherManager().acquire();
		try {
			/*
			 * SortedSetDocValues sortedSetValues =
			 * MultiDocValues.getSortedSetValues(searcher.getIndexReader(), fieldName);
			 * if (sortedSetValues == null || sortedSetValues.nextDoc() ==
			 * SortedSetDocValues.NO_MORE_DOCS) {
			 * return Collections.emptySet();
			 * }
			 */

			Set<String> fieldValues = new HashSet<>();

			SortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(searcher.getIndexReader(),
					facetConfig);

			FacetsCollector collector = new FacetsCollector();

			TermQuery collectionQuery = new TermQuery(new Term("_collection", COLLECTION_DOKUMENTE));

			FacetsCollector.search(searcher, collectionQuery, 10, collector);
			Facets facets = new SortedSetDocValuesFacetCounts(state, collector);
			FacetResult allChildren = facets.getAllChildren(fieldName);

			Stream.of(allChildren.labelValues).forEach(lv -> fieldValues.add(lv.label));

			return fieldValues;
		} finally {
			luceneIndexAdapter.getIndex(DB_SEARCH, COLLECTION_DOKUMENTE).getSearcherManager().release(searcher);
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

		return luceneIndexAdapter.size(DB_SEARCH, collection);
	}

	private void assertCollectionSize(final String collection, int size) throws IOException {
		int count = getSize(collection);
		Assertions.assertThat(count).isEqualTo(size);
	}

}
