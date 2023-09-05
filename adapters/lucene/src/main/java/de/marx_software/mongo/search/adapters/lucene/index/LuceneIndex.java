package de.marx_software.mongo.search.adapters.lucene.index;

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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.NRTCachingDirectory;

/**
 *
 * @author t.marx
 */
public class LuceneIndex {

	private IndexWriter writer;
	private SearcherManager searcherManager;
	
	private final LuceneIndexConfiguration configuration;
	
	public LuceneIndex(LuceneIndexConfiguration configuration) {
		this.configuration = configuration;
	}

	public LuceneIndex open() throws IOException {
		
		configuration.getStorage().open();
		
		NRTCachingDirectory cachedFSDir = new NRTCachingDirectory(configuration.getStorage().getDirectory(), 5.0, 60.0);
		IndexWriterConfig conf = new IndexWriterConfig(configuration.getDefaultAnalyzer());
		writer = new IndexWriter(cachedFSDir, conf);
		
		searcherManager = new SearcherManager(writer, true, false, new SearcherFactory());
		
		return this;
	}
	
	public SearcherManager getSearcherManager () {
		return searcherManager;
	}
	
	public void index (final Document document) throws IOException {
		writer.addDocument(document);
	}
	
	public void deleteDocuments (Query query) throws IOException {
		writer.deleteDocuments(query);
	}

	public void commit() {
		try {
			writer.commit();
			writer.maybeMerge();
			searcherManager.maybeRefresh();
		} catch (IOException ex) {
			Logger.getLogger(LuceneIndex.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	
	public void close() throws Exception {
		if (searcherManager != null) {
			searcherManager.close();
		}
		if (writer != null) {
			writer.close();
		}
		
		configuration.getStorage().close();
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
