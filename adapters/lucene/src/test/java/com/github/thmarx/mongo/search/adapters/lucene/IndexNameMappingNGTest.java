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
public class IndexNameMappingNGTest extends AbstractContainerTest {

	MongoSearch mongoSearch;

	private MongoDatabase database;

	private LuceneIndexAdapter luceneIndexAdapter;

	private FacetsConfig facetConfig = new FacetsConfig();

	private static final String COLLECTION_DOCUMENTS = "documente";
	private static final String COLLECTION_BOOKS = "books";
	private static final String DB_SEARCH = "search";

	@BeforeMethod
	public void setup() throws IOException, InterruptedException {

		database = mongoClient.getDatabase(DB_SEARCH);

		FileUtil.delete(Path.of("target/index"));
		Files.createDirectories(Path.of("target/index"));

		Stream.of(COLLECTION_DOCUMENTS, COLLECTION_BOOKS).forEach(collection -> {
			if (database.getCollection(collection) != null) {
				database.getCollection(collection).drop();
			}
			database.createCollection(collection);
		});

		LuceneIndexConfiguration configuration = new LuceneIndexConfiguration();

		PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(new StandardAnalyzer(),
				Map.of("name", new GermanAnalyzer()));
		configuration.setAnalyzer(perFieldAnalyzerWrapper);
		configuration.setCommitDelaySeconds(1);
		configuration.setStorage(new FileSystemStorage(Path.of("target/index")));
		configuration.setFacetsConfig(facetConfig);
		// index multiple collections into same index
		configuration.setIndexNameMapper((db, col) -> db);
		configuration.setDocumentExtender((source, target) -> {
			var values = ListFieldMappers.toString("tags", source);
			if (values != null && !values.isEmpty()) {
				values.forEach(value -> {
					target.add(new SortedSetDocValuesFacetField("tags", value));
				});
			}
		});

		configuration.addFieldConfiguration(COLLECTION_DOCUMENTS, LuceneFieldConfiguration.builder()
				.fieldName("name")
				.indexFieldName("name")
				.stored(true)
				.mapper(FieldMappers::toString)
				.build());

		configuration.addFieldConfiguration(COLLECTION_BOOKS, LuceneFieldConfiguration.builder()
				.fieldName("author")
				.indexFieldName("author")
				.stored(true)
				.keyword(true)
				.mapper(FieldMappers::toString)
				.build());
		
		luceneIndexAdapter = new LuceneIndexAdapter(configuration);
		luceneIndexAdapter.open();

		mongoSearch = new MongoSearch();
		mongoSearch.open(luceneIndexAdapter, database, List.of(COLLECTION_DOCUMENTS, COLLECTION_BOOKS));
		
		Thread.sleep(500);
	}

	@AfterMethod
	public void shutdown() throws Exception {
		mongoSearch.close();
	}

	@Test
	public void test_insert() throws IOException, InterruptedException {

		assertCollectionSize(COLLECTION_DOCUMENTS, 0);
		insertDocument(COLLECTION_DOCUMENTS, Map.of("name", "thorsten"));
		insertDocument(COLLECTION_BOOKS, Map.of("author", "lara"));

		Awaitility.await().atMost(10, TimeUnit.MINUTES).until(() -> getSize(COLLECTION_DOCUMENTS) == 1);
		Awaitility.await().atMost(10, TimeUnit.MINUTES).until(() -> getSize(COLLECTION_BOOKS) == 1);

		assertCollectionSize(COLLECTION_DOCUMENTS, 1);
		assertCollectionSize(COLLECTION_BOOKS, 1);
		
		luceneIndexAdapter.getIndex(DB_SEARCH, COLLECTION_BOOKS).commit();
		IndexSearcher searcher = luceneIndexAdapter.getIndex(DB_SEARCH, COLLECTION_BOOKS).getSearcherManager().acquire();
		try {
			Assertions.assertThat(searcher.getIndexReader().numDocs()).isEqualTo(2);
		} finally {
			luceneIndexAdapter.getIndex(DB_SEARCH, COLLECTION_BOOKS).getSearcherManager().release(searcher);
		}
	}
	
	@Test
	public void test_drop_single_collectino() throws IOException, InterruptedException {

		assertCollectionSize(COLLECTION_DOCUMENTS, 0);
		insertDocument(COLLECTION_DOCUMENTS, Map.of("name", "thorsten"));
		insertDocument(COLLECTION_BOOKS, Map.of("author", "lara"));

		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_DOCUMENTS) == 1);
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_BOOKS) == 1);

		assertCollectionSize(COLLECTION_DOCUMENTS, 1);
		assertCollectionSize(COLLECTION_BOOKS, 1);
		
		luceneIndexAdapter.getIndex(DB_SEARCH, COLLECTION_BOOKS).commit();
		IndexSearcher searcher = luceneIndexAdapter.getIndex(DB_SEARCH, COLLECTION_BOOKS).getSearcherManager().acquire();
		try {
			Assertions.assertThat(searcher.getIndexReader().numDocs()).isEqualTo(2);
		} finally {
			luceneIndexAdapter.getIndex(DB_SEARCH, COLLECTION_BOOKS).getSearcherManager().release(searcher);
		}
		
		database.getCollection(COLLECTION_BOOKS).drop();
		
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> getSize(COLLECTION_BOOKS) == 0);
		
		assertCollectionSize(COLLECTION_DOCUMENTS, 1);
		assertCollectionSize(COLLECTION_BOOKS, 0);
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
