/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.search.adapters.lucene;

/*-
 * #%L
 * monog-search-adapters-lucene
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
import com.github.thmarx.mongo.search.adapter.AbstractIndexAdapter;
import com.github.thmarx.mongo.search.adapters.lucene.index.Documents;
import com.github.thmarx.mongo.search.adapters.lucene.index.LuceneIndex;
import com.github.thmarx.mongo.search.adapters.lucene.index.LuceneIndexConfiguration;
import com.github.thmarx.mongo.search.index.commands.Command;
import com.github.thmarx.mongo.search.index.commands.DeleteCommand;
import com.github.thmarx.mongo.search.index.commands.DropCollectionCommand;
import com.github.thmarx.mongo.search.index.commands.DropDatabaseCommand;
import com.github.thmarx.mongo.search.index.commands.IndexCommand;
import com.github.thmarx.mongo.search.index.utils.PausableThread;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
public class LuceneIndexAdapter extends AbstractIndexAdapter<LuceneIndexConfiguration> {

	LuceneIndex luceneIndex;

	private Thread queueWorkerThread;

	private PausableThread queueWorker;

	private Documents documentHelper;

	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	
	public LuceneIndexAdapter(final LuceneIndexConfiguration configuration) {
		super(configuration);

		this.documentHelper = new Documents(configuration);
	}

	@Override
	public void indexDocument(String database, String collection, Document document) throws IOException {
		org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
		doc.add(new StringField("_id", document.getObjectId("_id").toString(), Field.Store.YES));
		doc.add(new StringField("_collection", collection, Field.Store.YES));
		doc.add(new StringField("_database", collection, Field.Store.YES));

		luceneIndex.index(doc);
	}

	
	public void commit() {
		luceneIndex.commit();
	}
	
	public LuceneIndex getIndex() {
		return luceneIndex;
	}

	@Override
	public void close() throws Exception {
		scheduler.shutdown();
		queueWorker.stop();
		queueWorkerThread.interrupt();
		luceneIndex.close();
	}

	@Override
	public void startQueueWorker() {
		this.queueWorker.unpause();
	}

	public void open() throws IOException {

		luceneIndex = new LuceneIndex(configuration);
		luceneIndex.open();

		scheduler.scheduleWithFixedDelay(() -> {
			luceneIndex.commit();
		}, 1, 1, TimeUnit.SECONDS);
		
		queueWorker = new PausableThread(true) {
			@Override
			public void update() {
				try {
					Command command = getCommandQueue().take();

					if (command instanceof IndexCommand index) {
						org.apache.lucene.document.Document doc = documentHelper.build(index);

						Query query = new BooleanQuery.Builder()
								.add(new TermQuery(new Term("_id", index.uid())), BooleanClause.Occur.MUST)
								.add(new TermQuery(new Term("_collection", index.collection())), BooleanClause.Occur.MUST)
								.add(new TermQuery(new Term("_database", index.database())), BooleanClause.Occur.MUST)
								.build();
						luceneIndex.deleteDocuments(query);

						luceneIndex.index(doc);
					} else if (command instanceof DeleteCommand delete) {
						Query query = new BooleanQuery.Builder()
								.add(new TermQuery(new Term("_id", delete.uid())), BooleanClause.Occur.MUST)
								.add(new TermQuery(new Term("_collection", delete.collection())), BooleanClause.Occur.MUST)
								.add(new TermQuery(new Term("_database", delete.database())), BooleanClause.Occur.MUST)
								.build();
						luceneIndex.deleteDocuments(query);
					} else if (command instanceof DropCollectionCommand dropCollection) {
						Query query = new BooleanQuery.Builder()
								.add(new TermQuery(new Term("_collection", dropCollection.collection())), BooleanClause.Occur.MUST)
								.build();
						luceneIndex.deleteDocuments(query);
					} else if (command instanceof DropDatabaseCommand dropDatabase) {
						Query query = new BooleanQuery.Builder()
								.add(new TermQuery(new Term("_database", dropDatabase.database())), BooleanClause.Occur.MUST)
								.build();
						luceneIndex.deleteDocuments(query);
					}
				} catch (InterruptedException ex) {
					// nothing to do
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}
			}
		};
		queueWorkerThread = new Thread(queueWorker);
		queueWorkerThread.start();
	}

	public int size(String database, String collection) {
		try {
			return luceneIndex.size(database, collection);
		} catch (IOException ex) {
			return 0;
		}
	}

}
