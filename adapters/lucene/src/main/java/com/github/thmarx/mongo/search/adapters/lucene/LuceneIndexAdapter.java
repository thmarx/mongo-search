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
import com.github.thmarx.mongo.search.index.messages.Message;
import com.github.thmarx.mongo.search.index.messages.DeleteMessage;
import com.github.thmarx.mongo.search.index.messages.DropCollectionMessage;
import com.github.thmarx.mongo.search.index.messages.DropDatabaseMessage;
import com.github.thmarx.mongo.search.index.messages.InsertMessage;
import com.github.thmarx.mongo.search.index.utils.PausableThread;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
public class LuceneIndexAdapter extends AbstractIndexAdapter<LuceneIndexConfiguration> {

	LuceneIndex luceneIndex;

	private Thread queueWorkerThread;

	private PausableThread queueWorker;

	private final Documents documentHelper;

	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

	public LuceneIndexAdapter(final LuceneIndexConfiguration configuration) {
		super(configuration);

		this.documentHelper = new Documents(configuration);
	}

	@Override
	public void indexDocument(String database, String collection, Document document) throws IOException {
		var uid = document.getObjectId("_id").toString();
		
		Query query = new BooleanQuery.Builder()
				.add(new TermQuery(new Term("_id", uid)), BooleanClause.Occur.MUST)
				.add(new TermQuery(new Term("_collection", collection)), BooleanClause.Occur.MUST)
				.add(new TermQuery(new Term("_database", database)), BooleanClause.Occur.MUST)
				.build();
		luceneIndex.deleteDocuments(query);

		org.apache.lucene.document.Document doc = documentHelper.build(database, collection, uid, document);
		luceneIndex.index(doc);
	}

	@Override
	public void clearCollection(String database, String collection) throws IOException {
		
		Query query = new BooleanQuery.Builder()
				.add(new TermQuery(new Term("_collection", collection)), BooleanClause.Occur.MUST)
				.add(new TermQuery(new Term("_database", database)), BooleanClause.Occur.MUST)
				.build();
		luceneIndex.deleteDocuments(query);
	}

	public void commit() {
		luceneIndex.commit();
	}

	public LuceneIndex getIndex() {
		return luceneIndex;
	}

	@Override
	public void close() throws Exception {
		log.debug("close lucene index adapter");
		log.debug("stop scheduler");
		scheduler.shutdownNow();
		log.debug("stop queue worker");
		queueWorker.stop();
		log.debug("stop queue worker thread");
		queueWorkerThread.interrupt();
		log.debug("stop lucene index");
		luceneIndex.close();
	}

	@Override
	public void startQueueWorker() {
		this.queueWorker.unpause();
	}

	@Override
	public void pauseQueueWorker() {
		this.queueWorker.pause();
	}

	public void open() throws IOException {

		luceneIndex = new LuceneIndex(configuration);
		luceneIndex.open();

		scheduler.scheduleWithFixedDelay(() -> {
			try {
				log.trace("commit index");
				luceneIndex.commit();
				log.trace("commited");
			} catch (Exception e) {
				log.error("", e);
			}
		}, 1, configuration.getCommitDelaySeconds(), TimeUnit.SECONDS);

		queueWorker = new PausableThread(true) {
			@Override
			public void update() {
				try {
					Message command = getMessageQueue().take();

					if (command instanceof InsertMessage index) {
						org.apache.lucene.document.Document doc = documentHelper.build(index);

						Query query = new BooleanQuery.Builder()
								.add(new TermQuery(new Term("_id", index.uid())), BooleanClause.Occur.MUST)
								.add(new TermQuery(new Term("_collection", index.collection())),
										BooleanClause.Occur.MUST)
								.add(new TermQuery(new Term("_database", index.database())), BooleanClause.Occur.MUST)
								.build();
						luceneIndex.deleteDocuments(query);

						luceneIndex.index(doc);
					} else if (command instanceof DeleteMessage delete) {
						Query query = new BooleanQuery.Builder()
								.add(new TermQuery(new Term("_id", delete.uid())), BooleanClause.Occur.MUST)
								.add(new TermQuery(new Term("_collection", delete.collection())),
										BooleanClause.Occur.MUST)
								.add(new TermQuery(new Term("_database", delete.database())), BooleanClause.Occur.MUST)
								.build();
						luceneIndex.deleteDocuments(query);
					} else if (command instanceof DropCollectionMessage dropCollection) {
						Query query = new BooleanQuery.Builder()
								.add(new TermQuery(new Term("_collection", dropCollection.collection())),
										BooleanClause.Occur.MUST)
								.build();
						luceneIndex.deleteDocuments(query);
					} else if (command instanceof DropDatabaseMessage dropDatabase) {
						Query query = new BooleanQuery.Builder()
								.add(new TermQuery(new Term("_database", dropDatabase.database())),
										BooleanClause.Occur.MUST)
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
