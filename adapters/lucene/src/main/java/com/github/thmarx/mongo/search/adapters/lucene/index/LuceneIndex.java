package com.github.thmarx.mongo.search.adapters.lucene.index;

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

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;

/**
 *
 * @author t.marx
 */
@Slf4j
public class LuceneIndex {

	private IndexWriter writer;
	private SearcherManager searcherManager;
	
	private final LuceneIndexConfiguration configuration;
	private final String name;
	FacetsConfig facetsConfig = null;
	Directory directory;

	public LuceneIndex(String name, LuceneIndexConfiguration configuration) {
		this.name = name;
		this.configuration = configuration;
		this.facetsConfig = configuration.facetsConfig;
	}

	public LuceneIndex open() throws IOException {
		
		this.directory = configuration.getStorage().createDirectory(name);
		
		NRTCachingDirectory cachedFSDir = new NRTCachingDirectory(this.directory, 5.0, 60.0);
		IndexWriterConfig conf = new IndexWriterConfig(configuration.getAnalyzer());
		conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
		writer = new IndexWriter(cachedFSDir, conf);
		
		searcherManager = new SearcherManager(writer, true, false, new SearcherFactory());
		
		return this;
	}
	
	public SearcherManager getSearcherManager () {
		return searcherManager;
	}
	
	public void index (final Document document) throws IOException {
		if (facetsConfig != null) {
			writer.addDocument(facetsConfig.build(document));
		} else {
			writer.addDocument(document);
		}
		
	}
	
	public void deleteDocuments (Query query) throws IOException {
		writer.deleteDocuments(query);
	}

	public void commit() {
		try {
			writer.commit();
			searcherManager.maybeRefresh();
		} catch (IOException ex) {
			log.error("", ex);
		}
	}
	
	
	public void close() throws Exception {
		if (searcherManager != null) {
			searcherManager.close();
		}
		if (writer != null) {
			writer.close();
		}
		
		this.directory.close();
	}
	
	public int size(String database, String collection) throws IOException {
		var searcher = searcherManager.acquire();
		try {
			Query query = new BooleanQuery.Builder()
					.add(new TermQuery(new Term("_collection", collection)), BooleanClause.Occur.MUST)
					.add(new TermQuery(new Term("_database", database)), BooleanClause.Occur.MUST)
					.build();
			return searcher.count(query);
		} finally {
			searcherManager.release(searcher);
		}
	}
}
